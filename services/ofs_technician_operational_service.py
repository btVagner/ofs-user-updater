from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

from ofs.client import OFSClient
from ofs.config import get_ofs_root_resource_id


LOGGER = logging.getLogger(__name__)

TECHNICIAN_RESOURCE_TYPES = ("TCV", "TCP", "TCW")
ACTIVITY_STATUSES = (
    "pending",
    "enroute",
    "started",
    "suspended",
    "completed",
    "notdone",
    "cancelled",
)
OPEN_ACTIVITY_STATUSES = {"pending", "enroute", "started", "suspended"}
ROUTE_EVENTS = {
    "routeCreated",
    "routeUpdated",
    "routeActivated",
    "routeDeactivated",
    "routeReactivated",
}
ACTIVITY_EVENTS = {
    "activityStarted",
    "activityTravelStarted",
    "activityTravelStopped",
    "activitySuspended",
    "activityCompleted",
    "activityNotDone",
    "activityCanceled",
    "activityMoved",
}
REQUESTED_EVENTS = tuple(sorted(ROUTE_EVENTS | ACTIVITY_EVENTS))
ACTIVITY_FIELDS = (
    "activityId",
    "apptNumber",
    "resourceId",
    "date",
    "status",
    "activityType",
    "startTime",
    "endTime",
    "resourceTimeZoneIANA",
)
CURSOR_KEY = "technician_monitor"
LOCK_NAME = "ofs_technician_operational_worker"
CUSTOMER_HOME_ACTIVITY_CATEGORY = "customer_home"

SENSITIVE_INLINE_RE = re.compile(
    r"(?i)(authorization|client[_-]?secret|password|passwd|access[_-]?token|refresh[_-]?token|bearer)\s*[:=]\s*[^\s,;]+"
)




def _default_connection_factory():
    # Lazy import keeps parser/API unit tests independent from the optional
    # MySQL driver while preserving the project connection helper in runtime.
    from database.connection import get_connection

    return get_connection()

class OperationalError(RuntimeError):
    pass


class OperationalAlreadyRunning(OperationalError):
    pass


class OperationalAPIError(OperationalError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, retryable: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


class SubscriptionRecoveryRequired(OperationalError):
    pass


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def sanitize_operational_error(value: Any, limit: int = 500) -> str:
    """Retorna uma mensagem operacional curta sem credenciais reconhecíveis."""
    text = str(value or "")[:limit]
    return SENSITIVE_INLINE_RE.sub(lambda m: f"{m.group(1)}=<redacted>", text)


# Alias interno para manter as chamadas compactas no serviço.
_safe_error_text = sanitize_operational_error


def _parse_date(value: Any) -> Optional[date]:
    text = _clean(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = _clean(value)
    if not text:
        return None
    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
    )
    for fmt in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def _parse_local_datetime(value: Any) -> Optional[datetime]:
    """Preserva o relogio local recebido, descartando apenas o offset.

    Os campos operacionais de rota sao DATETIME sem timezone no read model.
    Diferentemente de ``_parse_datetime``, esta funcao nao converte o instante
    para UTC antes de remover o ``tzinfo``.
    """
    text = _clean(value)
    if not text:
        return None
    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
    )
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=None)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _combine_local(work_date: date, hhmm: Any) -> Optional[datetime]:
    text = _clean(hhmm)
    if not text:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed_time = datetime.strptime(text, fmt).time()
            return datetime.combine(work_date, parsed_time)
        except ValueError:
            continue
    return None


def _event_fingerprint(event_type: str, event: dict) -> str:
    details = event.get("activityDetails") or event.get("routeDetails") or {}
    changes = event.get("activityChanges") or event.get("routeChanges") or {}
    stable = {
        "eventType": event_type,
        "details": details if isinstance(details, dict) else {},
        "changes": changes if isinstance(changes, dict) else {},
    }
    return json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)[:1000]




def event_should_apply(last_event_at: Optional[datetime], last_fingerprint: Optional[str], event_at: datetime, fingerprint: str) -> bool:
    """Idempotência por entidade: descarta somente eventos mais antigos ou repetição exata.

    Eventos distintos no mesmo segundo continuam aplicáveis na ordem entregue pela Events API;
    replay da página é seguro porque a transação é atômica e a repetição exata finaliza
    no mesmo estado.
    """
    if last_event_at and event_at < last_event_at:
        return False
    if last_event_at == event_at and last_fingerprint == fingerprint:
        return False
    return True


def retention_cutoff(today: date, retention_days: int = 7) -> date:
    return today - timedelta(days=max(retention_days, 1) - 1)


def is_retained_work_date(work_date: date, today: date, retention_days: int = 7) -> bool:
    return retention_cutoff(today, retention_days) <= work_date <= today

def normalize_activity(item: dict, *, reconciled_at: Optional[datetime] = None) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    activity_id = _clean(item.get("activityId"))
    work_date = _parse_date(item.get("date"))
    if not activity_id or not work_date:
        return None
    status = (_clean(item.get("status")) or "unknown").lower()
    return {
        "activity_id": activity_id,
        "work_date": work_date,
        "resource_id": _clean(item.get("resourceId")),
        "status": status,
        "appt_number": _clean(item.get("apptNumber")),
        "activity_type": _clean(item.get("activityType")),
        "resource_timezone_iana": _clean(item.get("resourceTimeZoneIANA")),
        "last_event_at": None,
        "last_event_type": None,
        "last_event_fingerprint": None,
        "last_reconciled_at": reconciled_at,
    }


def normalize_calendar_item(item: dict) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    resource_id = _clean(item.get("resourceId"))
    work_date = _parse_date(item.get("date"))
    if not resource_id or not work_date:
        return None
    regular = item.get("regular") if isinstance(item.get("regular"), dict) else None
    on_call = item.get("on-call") if isinstance(item.get("on-call"), dict) else None
    selected = regular or on_call or {}
    record_type = _clean(selected.get("recordType"))
    # Oracle can return recordType=working inside the top-level on-call object.
    # Preserve the scheduling semantic in the compact read model so the alert
    # classifier does not mistake plantão for a mandatory regular shift.
    if regular is None and on_call is not None and (record_type or "").lower() == "working":
        record_type = "on-call"
    return {
        "resource_id": resource_id,
        "work_date": work_date,
        "calendar_record_type": record_type,
        "calendar_start_at": _combine_local(work_date, selected.get("workTimeStart")),
        "calendar_end_at": _combine_local(work_date, selected.get("workTimeEnd")),
        "non_working_reason": _clean(selected.get("nonWorkingReason")),
    }


def normalize_route_baseline(resource_id: str, work_date: date, payload: dict, *, reconciled_at: datetime) -> dict:
    started = _parse_local_datetime(payload.get("routeStartTime"))
    reactivated = _parse_local_datetime(payload.get("routeReactivationTime"))
    ended = _parse_local_datetime(payload.get("routeEndTime"))
    if ended:
        state = "ended"
    elif started or reactivated:
        state = "active"
    else:
        state = "not_started"
    return {
        "resource_id": str(resource_id),
        "work_date": work_date,
        "route_state": state,
        "route_state_raw": "baseline_route_response",
        "route_started_at": started,
        "route_reactivated_at": reactivated,
        "route_ended_at": ended,
        "route_last_event_at": None,
        "route_last_event_type": None,
        "last_reconciled_at": reconciled_at,
    }


def parse_activity_event(event: dict) -> Optional[dict]:
    event_type = _clean(event.get("eventType"))
    if event_type not in ACTIVITY_EVENTS:
        return None
    details = event.get("activityDetails") if isinstance(event.get("activityDetails"), dict) else {}
    changes = event.get("activityChanges") if isinstance(event.get("activityChanges"), dict) else {}
    activity_id = _clean(details.get("activityId") or changes.get("activityId"))
    if not activity_id:
        return None

    original_date = _parse_date(details.get("date"))
    original_resource = _clean(details.get("resourceId"))
    destination_date = _parse_date(changes.get("date")) if event_type == "activityMoved" else None
    destination_resource = _clean(changes.get("resourceId")) if event_type == "activityMoved" else None

    explicit_status = _clean(changes.get("status") or details.get("status"))
    fallback_status = {
        "activityStarted": "started",
        "activityTravelStarted": "enroute",
        "activitySuspended": "suspended",
        "activityCompleted": "completed",
        "activityNotDone": "notdone",
        "activityCanceled": "cancelled",
    }.get(event_type)
    status = (explicit_status or fallback_status)
    if event_type == "activityTravelStopped" and not status:
        # Sem status destino o evento não pode ser aplicado com segurança.
        return {
            "kind": "unsafe_activity_event",
            "event_type": event_type,
            "activity_id": activity_id,
            "reason": "activityTravelStopped sem status destino",
        }

    return {
        "kind": "activity",
        "event_type": event_type,
        "event_at": _parse_datetime(event.get("time")) or utc_now_naive(),
        "fingerprint": _event_fingerprint(event_type, event),
        "activity_id": activity_id,
        "original_work_date": original_date,
        "original_resource_id": original_resource,
        "work_date": destination_date or original_date,
        "resource_id": destination_resource or original_resource,
        "status": status.lower() if status else None,
        "appt_number": _clean(changes.get("apptNumber") or details.get("apptNumber")),
        "activity_type": _clean(changes.get("activityType") or details.get("activityType")),
        "resource_timezone_iana": _clean(changes.get("resourceTimeZoneIANA") or details.get("resourceTimeZoneIANA")),
    }


def parse_route_event(event: dict) -> Optional[dict]:
    event_type = _clean(event.get("eventType"))
    if event_type not in ROUTE_EVENTS:
        return None
    details = event.get("routeDetails") if isinstance(event.get("routeDetails"), dict) else {}
    changes = event.get("routeChanges") if isinstance(event.get("routeChanges"), dict) else {}
    resource_id = _clean(details.get("resourceId") or changes.get("resourceId"))
    work_date = _parse_date(details.get("date") or changes.get("date"))
    if not resource_id or not work_date:
        return None
    event_at = _parse_datetime(event.get("time")) or utc_now_naive()

    route_state = None
    if event_type == "routeActivated":
        route_state = "active"
    elif event_type == "routeReactivated":
        route_state = "active"
    elif event_type == "routeDeactivated":
        route_state = "ended"
    elif event_type in {"routeCreated", "routeUpdated"}:
        route_state = None

    return {
        "kind": "route",
        "event_type": event_type,
        "event_at": event_at,
        "fingerprint": _event_fingerprint(event_type, event),
        "resource_id": resource_id,
        "work_date": work_date,
        "route_state": route_state,
        "route_started_at": _parse_local_datetime(changes.get("activated")) if event_type == "routeActivated" else None,
        "route_reactivated_at": _parse_local_datetime(changes.get("reactivated")) if event_type == "routeReactivated" else None,
        "route_ended_at": _parse_local_datetime(changes.get("deactivated")) if event_type == "routeDeactivated" else None,
        "calendar_start_at": _combine_local(work_date, changes.get("calendarTimeFrom")),
        "calendar_end_at": _combine_local(work_date, changes.get("calendarTimeTo")),
        "resource_timezone": _clean(changes.get("timeZone")),
    }


@dataclass
class OperationalSettings:
    events_poll_seconds: int = 60
    activities_reconcile_seconds: int = 12 * 60
    calendars_reconcile_seconds: int = 45 * 60
    route_workers: int = 8
    request_timeout_seconds: int = 30
    request_retries: int = 4
    backoff_base_seconds: float = 1.0
    event_limit: int = 1000
    activities_limit: int = 10000
    retention_days: int = 7

    @classmethod
    def from_env(cls) -> "OperationalSettings":
        def int_env(name: str, default: int, minimum: int = 1) -> int:
            try:
                return max(int(os.getenv(name, default)), minimum)
            except (TypeError, ValueError):
                return default

        def float_env(name: str, default: float, minimum: float = 0.0) -> float:
            try:
                return max(float(os.getenv(name, default)), minimum)
            except (TypeError, ValueError):
                return default

        return cls(
            events_poll_seconds=int_env("OFS_OPERATIONAL_EVENTS_POLL_SECONDS", 60),
            activities_reconcile_seconds=int_env("OFS_OPERATIONAL_ACTIVITIES_RECONCILE_SECONDS", 720),
            calendars_reconcile_seconds=int_env("OFS_OPERATIONAL_CALENDARS_RECONCILE_SECONDS", 2700),
            route_workers=min(int_env("OFS_OPERATIONAL_ROUTE_WORKERS", 8), 16),
            request_timeout_seconds=min(int_env("OFS_OPERATIONAL_REQUEST_TIMEOUT_SECONDS", 30), 120),
            request_retries=min(int_env("OFS_OPERATIONAL_REQUEST_RETRIES", 4), 8),
            backoff_base_seconds=float_env("OFS_OPERATIONAL_BACKOFF_BASE_SECONDS", 1.0),
            event_limit=min(int_env("OFS_OPERATIONAL_EVENT_LIMIT", 1000), 1000),
            activities_limit=min(int_env("OFS_OPERATIONAL_ACTIVITIES_LIMIT", 10000), 100000),
            retention_days=7,
        )


class OFSOperationalAPI:
    def __init__(self, client: Optional[OFSClient] = None, settings: Optional[OperationalSettings] = None, sleep: Callable = time.sleep):
        self.client = client or OFSClient()
        self.settings = settings or OperationalSettings.from_env()
        self.sleep = sleep
        self.base_url = self.client.base_url.rstrip("/")

    def _request(self, method: str, path: str, *, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Optional[Exception] = None
        attempts = max(self.settings.request_retries, 1)
        for attempt in range(attempts):
            try:
                response = requests.request(
                    method,
                    url,
                    auth=self.client.auth,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    params=params,
                    json=json_body,
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code in {401, 403}:
                    raise OperationalAPIError(
                        f"OFS recusou autenticação/permissão ({response.status_code}) em {path}",
                        status_code=response.status_code,
                        retryable=False,
                    )
                if response.status_code == 429 or 500 <= response.status_code <= 599:
                    if attempt + 1 < attempts:
                        retry_after = response.headers.get("Retry-After")
                        try:
                            delay = float(retry_after) if retry_after else self.settings.backoff_base_seconds * (2 ** attempt)
                        except ValueError:
                            delay = self.settings.backoff_base_seconds * (2 ** attempt)
                        self.sleep(min(delay + random.random() * 0.2, 30.0))
                        continue
                    raise OperationalAPIError(
                        f"OFS indisponível após retries ({response.status_code}) em {path}",
                        status_code=response.status_code,
                        retryable=True,
                    )
                if not response.ok:
                    raise OperationalAPIError(
                        f"OFS retornou HTTP {response.status_code} em {path}",
                        status_code=response.status_code,
                        retryable=False,
                    )
                if response.status_code == 204 or not response.content:
                    return {}
                payload = response.json()
                if not isinstance(payload, dict):
                    raise OperationalAPIError(f"Resposta JSON inválida em {path}")
                return payload
            except OperationalAPIError:
                raise
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                if attempt + 1 < attempts:
                    self.sleep(min(self.settings.backoff_base_seconds * (2 ** attempt), 30.0))
                    continue
                raise OperationalAPIError(
                    f"Falha de rede/timeout em {path}: {_safe_error_text(exc)}",
                    retryable=True,
                ) from exc
            except (ValueError, requests.RequestException) as exc:
                last_exc = exc
                raise OperationalAPIError(f"Falha OFS em {path}: {_safe_error_text(exc)}") from exc
        raise OperationalAPIError(f"Falha OFS em {path}: {_safe_error_text(last_exc)}")

    def list_subscription(self, subscription_id: str) -> Optional[dict]:
        try:
            return self._request("GET", f"events/subscriptions/{subscription_id}")
        except OperationalAPIError as exc:
            if exc.status_code in {404, 410}:
                return None
            raise

    def create_subscription(self) -> Tuple[str, str]:
        payload = self._request(
            "POST",
            "events/subscriptions",
            json_body={
                "subscriptionTitle": "ofs_technician_operational_monitor",
                "subscriptionConfig": [
                    {"events": sorted(ROUTE_EVENTS)},
                    {"events": sorted(ACTIVITY_EVENTS), "fields": list(ACTIVITY_FIELDS)},
                ],
            },
        )
        subscription_id = _clean(payload.get("subscriptionId"))
        next_page = _clean(payload.get("nextPage"))
        if not subscription_id or not next_page:
            raise OperationalAPIError("Create subscription sem subscriptionId/nextPage")
        return subscription_id, next_page

    def get_events(self, subscription_id: str, page: str) -> Tuple[List[dict], str]:
        payload = self._request(
            "GET",
            "events",
            params={"subscriptionId": subscription_id, "page": page, "limit": self.settings.event_limit},
        )
        items = payload.get("items") or []
        if not isinstance(items, list):
            raise OperationalAPIError("Events retornou items inválido")
        next_page = _clean(payload.get("nextPage"))
        if not next_page:
            raise OperationalAPIError("Events retornou nextPage ausente")
        return [item for item in items if isinstance(item, dict)], next_page

    def get_calendars(self, root_resource_id: str, work_date: date) -> Tuple[List[dict], int]:
        rows: List[dict] = []
        offset = 0
        calls = 0
        while True:
            payload = self._request(
                "GET",
                "calendars",
                params={
                    "resources": root_resource_id,
                    "dateFrom": work_date.isoformat(),
                    "dateTo": work_date.isoformat(),
                    "includeChildren": "all",
                    "includeInactive": "false",
                    "limit": 100,
                    "offset": offset,
                },
            )
            calls += 1
            items = payload.get("items") or []
            if not isinstance(items, list):
                raise OperationalAPIError("Calendars retornou items inválido")
            rows.extend(item for item in items if isinstance(item, dict))
            if len(items) < 100:
                break
            offset += len(items)
        return rows, calls

    def get_activities(self, root_resource_id: str, work_date: date) -> Tuple[List[dict], int]:
        rows: List[dict] = []
        offset = 0
        calls = 0
        while True:
            payload = self._request(
                "GET",
                "activities",
                params={
                    "resources": root_resource_id,
                    "includeChildren": "all",
                    "dateFrom": work_date.isoformat(),
                    "dateTo": work_date.isoformat(),
                    "fields": ",".join(ACTIVITY_FIELDS),
                    "limit": self.settings.activities_limit,
                    "offset": offset,
                },
            )
            calls += 1
            items = payload.get("items") or []
            if not isinstance(items, list):
                raise OperationalAPIError("Activities retornou items inválido")
            rows.extend(item for item in items if isinstance(item, dict))
            has_more = payload.get("hasMore") is True
            if not has_more and len(items) < self.settings.activities_limit:
                break
            if not items:
                break
            offset += len(items)
        return rows, calls

    def get_route(self, resource_id: str, work_date: date) -> Tuple[dict, int]:
        first: Optional[dict] = None
        items: List[dict] = []
        offset = 0
        calls = 0
        while True:
            payload = self._request(
                "GET",
                f"resources/{resource_id}/routes/{work_date.isoformat()}",
                params={"activityFields": ",".join(ACTIVITY_FIELDS), "limit": 100, "offset": offset},
            )
            calls += 1
            if first is None:
                first = payload
            page_items = payload.get("items") or []
            if not isinstance(page_items, list):
                raise OperationalAPIError("Route retornou items inválido")
            items.extend(item for item in page_items if isinstance(item, dict))
            total = int(payload.get("totalResults") or len(items))
            if not page_items or len(items) >= total:
                break
            offset += len(page_items)
        merged = dict(first or {})
        merged["items"] = items
        return merged, calls


class MySQLOperationalRepository:
    def __init__(self, connection_factory: Optional[Callable] = None):
        self.connection_factory = connection_factory or _default_connection_factory

    def get_technicians(self, root_resource_id: str) -> List[dict]:
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT resource_id, resource_type, timezone
                FROM ofs_resource_hierarchy
                WHERE root_resource_id = %s
                  AND resource_type IN ('TCV','TCP','TCW')
                ORDER BY resource_id
                """,
                (root_resource_id,),
            )
            return list(cur.fetchall() or [])
        finally:
            cur.close()
            conn.close()

    def get_route_timestamp_rows(self, work_date: date) -> List[dict]:
        """Lista somente os campos necessarios para reconciliar uma data."""
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT work_date, resource_id, route_started_at, route_reactivated_at,
                       route_ended_at, route_last_event_type
                FROM ofs_technician_operational_state
                WHERE work_date=%s
                ORDER BY resource_id
                """,
                (work_date,),
            )
            return list(cur.fetchall() or [])
        finally:
            cur.close()
            conn.close()

    def update_route_timestamps(self, work_date: date, resource_id: str, values: dict, *, now: Optional[datetime] = None) -> int:
        """Atualiza somente timestamps de rota permitidos para uma linha exata."""
        allowed = ("route_started_at", "route_reactivated_at", "route_ended_at")
        fields = [field for field in allowed if field in values]
        unknown = set(values) - set(allowed)
        if unknown:
            raise ValueError(f"Campos de rota nao permitidos: {sorted(unknown)}")
        if not fields:
            return 0

        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            assignments = ",".join(f"{field}=%s" for field in fields)
            cur.execute(
                f"UPDATE ofs_technician_operational_state SET {assignments},updated_at=%s WHERE work_date=%s AND resource_id=%s",
                tuple(values[field] for field in fields) + (now or utc_now_naive(), work_date, str(resource_id)),
            )
            updated = max(int(cur.rowcount or 0), 0)
            if updated != 1:
                raise OperationalError(
                    f"Linha de rota nao encontrada para resource_id={resource_id} work_date={work_date.isoformat()}"
                )
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def get_cursor(self, cursor_key: str = CURSOR_KEY) -> Optional[dict]:
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM ofs_event_cursor WHERE cursor_key = %s", (cursor_key,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    def save_new_subscription(self, subscription_id: str, next_page: str, now: datetime, cursor_key: str = CURSOR_KEY) -> None:
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO ofs_event_cursor
                    (cursor_key, subscription_id, next_page, subscription_created_at, baseline_completed_at,
                     last_poll_success_at, last_event_at, last_error_code, last_error_message, updated_at)
                VALUES (%s,%s,%s,%s,NULL,NULL,NULL,NULL,NULL,%s)
                ON DUPLICATE KEY UPDATE
                    subscription_id=VALUES(subscription_id), next_page=VALUES(next_page),
                    subscription_created_at=VALUES(subscription_created_at), baseline_completed_at=NULL,
                    last_poll_success_at=NULL, last_event_at=NULL,
                    last_error_code=NULL, last_error_message=NULL, updated_at=VALUES(updated_at)
                """,
                (cursor_key, subscription_id, next_page, now, now),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def mark_baseline_completed(self, now: datetime, cursor_key: str = CURSOR_KEY) -> None:
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE ofs_event_cursor SET baseline_completed_at=%s, updated_at=%s WHERE cursor_key=%s",
                (now, now, cursor_key),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def ensure_technician_rows(self, technicians: Sequence[dict], work_date: date, now: datetime) -> None:
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            values = [
                (work_date, str(t["resource_id"]), _clean(t.get("timezone")), now)
                for t in technicians
            ]
            technician_ids = [value[1] for value in values]
            if technician_ids:
                placeholders = ",".join(["%s"] * len(technician_ids))
                cur.execute(
                    f"DELETE FROM ofs_technician_operational_state WHERE work_date=%s AND resource_id NOT IN ({placeholders})",
                    (work_date, *technician_ids),
                )
            cur.executemany(
                """
                INSERT INTO ofs_technician_operational_state
                    (work_date, resource_id, route_state, resource_timezone, updated_at)
                VALUES (%s,%s,'unknown',%s,%s)
                ON DUPLICATE KEY UPDATE
                    resource_timezone=COALESCE(VALUES(resource_timezone),resource_timezone),
                    updated_at=GREATEST(updated_at,VALUES(updated_at))
                """,
                values,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def apply_calendars(self, calendar_rows: Sequence[dict], technician_ids: set[str], now: datetime, work_date: Optional[date] = None) -> int:
        normalized = [normalize_calendar_item(x) for x in calendar_rows]
        normalized = [x for x in normalized if x and x["resource_id"] in technician_ids]
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            target_date = work_date or (normalized[0]["work_date"] if normalized else None)
            if target_date is not None:
                cur.execute(
                    """
                    UPDATE ofs_technician_operational_state
                    SET calendar_record_type=NULL, calendar_start_at=NULL, calendar_end_at=NULL,
                        non_working_reason=NULL, last_reconciled_at=%s, updated_at=%s
                    WHERE work_date=%s
                    """,
                    (now, now, target_date),
                )
            for row in normalized:
                cur.execute(
                    """
                    INSERT INTO ofs_technician_operational_state
                        (work_date,resource_id,route_state,calendar_record_type,calendar_start_at,calendar_end_at,
                         non_working_reason,last_reconciled_at,updated_at)
                    VALUES (%s,%s,'unknown',%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        calendar_record_type=VALUES(calendar_record_type),
                        calendar_start_at=VALUES(calendar_start_at),calendar_end_at=VALUES(calendar_end_at),
                        non_working_reason=VALUES(non_working_reason),last_reconciled_at=VALUES(last_reconciled_at),
                        updated_at=VALUES(updated_at)
                    """,
                    (
                        row["work_date"], row["resource_id"], row["calendar_record_type"],
                        row["calendar_start_at"], row["calendar_end_at"], row["non_working_reason"], now, now,
                    ),
                )
            conn.commit()
            return len(normalized)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def replace_activities_for_date(self, work_date: date, activities: Sequence[dict], technician_ids: set[str], reconciled_at: datetime) -> dict:
        normalized = [normalize_activity(x, reconciled_at=reconciled_at) for x in activities]
        normalized = [x for x in normalized if x]
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        affected_resources: set[str] = set()
        try:
            cur.execute(
                "SELECT DISTINCT resource_id FROM ofs_activity_operational_state WHERE work_date=%s AND resource_id IS NOT NULL",
                (work_date,),
            )
            affected_resources.update(str(r["resource_id"]) for r in (cur.fetchall() or []) if r.get("resource_id"))

            for row in normalized:
                if row.get("resource_id"):
                    affected_resources.add(row["resource_id"])
                cur.execute(
                    """
                    INSERT INTO ofs_activity_operational_state
                        (activity_id,work_date,resource_id,status,appt_number,activity_type,resource_timezone_iana,
                         last_event_at,last_event_type,last_event_fingerprint,last_reconciled_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,NULL,NULL,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        work_date=VALUES(work_date),resource_id=VALUES(resource_id),status=VALUES(status),
                        appt_number=VALUES(appt_number),activity_type=VALUES(activity_type),
                        resource_timezone_iana=VALUES(resource_timezone_iana),
                        last_reconciled_at=VALUES(last_reconciled_at),updated_at=VALUES(updated_at)
                    """,
                    (
                        row["activity_id"], row["work_date"], row["resource_id"], row["status"], row["appt_number"],
                        row["activity_type"], row["resource_timezone_iana"], reconciled_at, reconciled_at,
                    ),
                )

            cur.execute(
                "DELETE FROM ofs_activity_operational_state WHERE work_date=%s AND (last_reconciled_at IS NULL OR last_reconciled_at < %s)",
                (work_date, reconciled_at),
            )
            deleted = max(int(cur.rowcount or 0), 0)
            self._recompute_counts_cur(cur, work_date, affected_resources & technician_ids, reconciled_at)
            self._update_iana_from_activities_cur(cur, work_date, affected_resources & technician_ids, reconciled_at)
            conn.commit()
            return {"activities": len(normalized), "deleted": deleted, "affected_resources": len(affected_resources & technician_ids)}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _update_iana_from_activities_cur(self, cur, work_date: date, resource_ids: set[str], now: datetime) -> None:
        for resource_id in resource_ids:
            cur.execute(
                """
                SELECT resource_timezone_iana
                FROM ofs_activity_operational_state
                WHERE work_date=%s AND resource_id=%s AND resource_timezone_iana IS NOT NULL
                ORDER BY updated_at DESC LIMIT 1
                """,
                (work_date, resource_id),
            )
            row = cur.fetchone()
            if row and row.get("resource_timezone_iana"):
                cur.execute(
                    """
                    UPDATE ofs_technician_operational_state
                    SET resource_timezone_iana=%s, updated_at=%s
                    WHERE work_date=%s AND resource_id=%s
                    """,
                    (row["resource_timezone_iana"], now, work_date, resource_id),
                )

    def _recompute_counts_cur(self, cur, work_date: date, resource_ids: Iterable[str], now: datetime) -> dict:
        """Recompõe contadores Casa Cliente usando o mapa de tipos como fonte de verdade.

        O read model de atividades continua preservando tipos internal/redes para auditoria e
        reconciliação. Somente a derivação dos contadores operacionais é filtrada aqui.
        `include_in_bi` não participa deste domínio.
        """
        resource_ids = sorted({_clean(resource_id) for resource_id in resource_ids if _clean(resource_id)})
        if not resource_ids:
            return {"technicians_recomputed": 0, "eligible_activities": 0, "open_activities": 0}

        placeholders = ",".join(["%s"] * len(resource_ids))
        cur.execute(
            f"""
            SELECT a.resource_id, a.status, COUNT(*) AS total
            FROM ofs_activity_operational_state a
            INNER JOIN ofs_activity_type_map atm
                    ON atm.code COLLATE utf8mb4_unicode_ci = a.activity_type
                   AND atm.category = %s
                   AND atm.is_active = 1
            WHERE a.work_date = %s
              AND a.resource_id IN ({placeholders})
            GROUP BY a.resource_id, a.status
            """,
            (CUSTOMER_HOME_ACTIVITY_CATEGORY, work_date, *resource_ids),
        )

        counts_by_resource = {resource_id: {} for resource_id in resource_ids}
        eligible_activities = 0
        for row in cur.fetchall() or []:
            resource_id = _clean(row.get("resource_id"))
            status = (_clean(row.get("status")) or "unknown").lower()
            total = int(row.get("total") or 0)
            if resource_id in counts_by_resource:
                counts_by_resource[resource_id][status] = total
                eligible_activities += total

        update_rows = []
        open_activities = 0
        for resource_id in resource_ids:
            counts = counts_by_resource[resource_id]
            values = [counts.get(status, 0) for status in ACTIVITY_STATUSES]
            open_count = sum(counts.get(status, 0) for status in OPEN_ACTIVITY_STATUSES)
            open_activities += open_count
            update_rows.append((*values, open_count, now, now, work_date, resource_id))

        cur.executemany(
            """
            UPDATE ofs_technician_operational_state
            SET pending_count=%s,enroute_count=%s,started_count=%s,suspended_count=%s,
                completed_count=%s,notdone_count=%s,cancelled_count=%s,open_activity_count=%s,
                last_reconciled_at=%s,updated_at=%s
            WHERE work_date=%s AND resource_id=%s
            """,
            update_rows,
        )
        return {
            "technicians_recomputed": len(resource_ids),
            "eligible_activities": eligible_activities,
            "open_activities": open_activities,
        }

    def recompute_counts_for_date(self, work_date: date, now: Optional[datetime] = None) -> dict:
        """Recompõe localmente os contadores do dia sem qualquer chamada Oracle/OFS."""
        now = now or utc_now_naive()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT resource_id FROM ofs_technician_operational_state WHERE work_date=%s ORDER BY resource_id",
                (work_date,),
            )
            resource_ids = [row.get("resource_id") for row in (cur.fetchall() or []) if row.get("resource_id")]
            result = self._recompute_counts_cur(cur, work_date, resource_ids, now)
            conn.commit()
            return {"work_date": work_date.isoformat(), **result}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def apply_route_baseline(self, route_rows: Sequence[dict], now: datetime) -> int:
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            for row in route_rows:
                cur.execute(
                    """
                    INSERT INTO ofs_technician_operational_state
                        (work_date,resource_id,route_state,route_state_raw,route_started_at,route_reactivated_at,
                         route_ended_at,route_last_event_at,route_last_event_type,last_reconciled_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        route_state=VALUES(route_state),route_state_raw=VALUES(route_state_raw),
                        route_started_at=VALUES(route_started_at),route_reactivated_at=VALUES(route_reactivated_at),
                        route_ended_at=VALUES(route_ended_at),last_reconciled_at=VALUES(last_reconciled_at),updated_at=VALUES(updated_at)
                    """,
                    (
                        row["work_date"], row["resource_id"], row["route_state"], row["route_state_raw"],
                        row["route_started_at"], row["route_reactivated_at"], row["route_ended_at"],
                        row.get("route_last_event_at"), row.get("route_last_event_type"), row["last_reconciled_at"], now,
                    ),
                )
            conn.commit()
            return len(route_rows)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def apply_event_page(self, subscription_id: str, current_page: str, next_page: str, events: Sequence[dict], now: datetime, technician_ids: set[str], *, today: Optional[date] = None, retention_days: int = 7) -> dict:
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        applied = 0
        ignored = 0
        unsafe = 0
        affected: set[Tuple[date, str]] = set()
        max_event_at: Optional[datetime] = None
        retention_today = today or date.today()
        try:
            cur.execute(
                "SELECT subscription_id,next_page FROM ofs_event_cursor WHERE cursor_key=%s FOR UPDATE",
                (CURSOR_KEY,),
            )
            cursor_row = cur.fetchone()
            if not cursor_row or cursor_row.get("subscription_id") != subscription_id:
                raise SubscriptionRecoveryRequired("Subscription mudou antes do commit da página")
            if cursor_row.get("next_page") != current_page:
                raise SubscriptionRecoveryRequired("Cursor local mudou antes do commit da página")

            for event in events:
                op = parse_activity_event(event) or parse_route_event(event)
                if not op:
                    ignored += 1
                    continue
                if op.get("kind") == "unsafe_activity_event":
                    unsafe += 1
                    LOGGER.warning(
                        "Evento OFS não aplicado com segurança: type=%s activity_id=%s reason=%s",
                        op.get("event_type"),
                        op.get("activity_id"),
                        _safe_error_text(op.get("reason")),
                    )
                    continue
                max_event_at = max(max_event_at, op["event_at"]) if max_event_at else op["event_at"]
                if op["kind"] == "activity":
                    result = self._apply_activity_event_cur(
                        cur, op, now, technician_ids,
                        today=retention_today, retention_days=retention_days,
                    )
                    applied += int(result["applied"])
                    ignored += int(not result["applied"])
                    affected.update(result["affected"])
                else:
                    if not is_retained_work_date(op["work_date"], retention_today, retention_days):
                        ignored += 1
                        continue
                    was_applied = self._apply_route_event_cur(cur, op, now, technician_ids)
                    applied += int(was_applied)
                    ignored += int(not was_applied)

            for work_date, resource_id in affected:
                if resource_id in technician_ids:
                    self._recompute_counts_cur(cur, work_date, {resource_id}, now)
                    self._update_iana_from_activities_cur(cur, work_date, {resource_id}, now)

            cur.execute(
                """
                UPDATE ofs_event_cursor
                SET next_page=%s,last_poll_success_at=%s,last_event_at=COALESCE(%s,last_event_at),
                    last_error_code=NULL,last_error_message=NULL,updated_at=%s
                WHERE cursor_key=%s
                """,
                (next_page, now, max_event_at, now, CURSOR_KEY),
            )
            conn.commit()
            return {"applied": applied, "ignored": ignored, "unsafe": unsafe, "events": len(events), "page_changed": next_page != current_page}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _apply_activity_event_cur(self, cur, op: dict, now: datetime, technician_ids: set[str], *, today: Optional[date] = None, retention_days: int = 7) -> dict:
        cur.execute(
            "SELECT * FROM ofs_activity_operational_state WHERE activity_id=%s FOR UPDATE",
            (op["activity_id"],),
        )
        current = cur.fetchone()
        if current:
            last_event_at = current.get("last_event_at")
            if not event_should_apply(last_event_at, current.get("last_event_fingerprint"), op["event_at"], op["fingerprint"]):
                return {"applied": False, "affected": set()}

        old_date = current.get("work_date") if current else op.get("original_work_date")
        old_resource = _clean(current.get("resource_id")) if current else op.get("original_resource_id")
        new_date = op.get("work_date") or old_date
        new_resource = op.get("resource_id") or old_resource
        new_status = op.get("status") or (current.get("status") if current else None) or "unknown"
        if not new_date:
            return {"applied": False, "affected": set()}

        retention_today = today or date.today()
        new_date_retained = is_retained_work_date(new_date, retention_today, retention_days)
        affected: set[Tuple[date, str]] = set()
        if old_date and old_resource and is_retained_work_date(old_date, retention_today, retention_days):
            affected.add((old_date, old_resource))

        # O read model operacional só mantém hoje..hoje-6. Um activityMoved para
        # data futura/expirada deve retirar a atividade do estado corrente, mas não
        # materializar a data fora da janela. A reconciliação do respectivo dia a
        # recriará quando ele entrar na janela operacional.
        if not new_date_retained:
            if current:
                cur.execute("DELETE FROM ofs_activity_operational_state WHERE activity_id=%s", (op["activity_id"],))
                return {"applied": True, "affected": affected}
            return {"applied": False, "affected": affected}

        if new_resource in technician_ids:
            cur.execute(
                """
                INSERT INTO ofs_technician_operational_state (work_date,resource_id,route_state,updated_at)
                VALUES (%s,%s,'unknown',%s)
                ON DUPLICATE KEY UPDATE updated_at=GREATEST(updated_at,VALUES(updated_at))
                """,
                (new_date, new_resource, now),
            )

        cur.execute(
            """
            INSERT INTO ofs_activity_operational_state
                (activity_id,work_date,resource_id,status,appt_number,activity_type,resource_timezone_iana,
                 last_event_at,last_event_type,last_event_fingerprint,last_reconciled_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s)
            ON DUPLICATE KEY UPDATE
                work_date=VALUES(work_date),resource_id=VALUES(resource_id),status=VALUES(status),
                appt_number=COALESCE(VALUES(appt_number),appt_number),activity_type=COALESCE(VALUES(activity_type),activity_type),
                resource_timezone_iana=COALESCE(VALUES(resource_timezone_iana),resource_timezone_iana),
                last_event_at=VALUES(last_event_at),last_event_type=VALUES(last_event_type),
                last_event_fingerprint=VALUES(last_event_fingerprint),updated_at=VALUES(updated_at)
            """,
            (
                op["activity_id"], new_date, new_resource, new_status, op.get("appt_number"), op.get("activity_type"),
                op.get("resource_timezone_iana"), op["event_at"], op["event_type"], op["fingerprint"], now,
            ),
        )
        if new_resource:
            affected.add((new_date, new_resource))
        return {"applied": True, "affected": affected}

    def _apply_route_event_cur(self, cur, op: dict, now: datetime, technician_ids: set[str]) -> bool:
        if op["resource_id"] not in technician_ids:
            return False
        cur.execute(
            "SELECT route_last_event_at,route_last_event_type,route_last_event_fingerprint,route_state FROM ofs_technician_operational_state WHERE work_date=%s AND resource_id=%s FOR UPDATE",
            (op["work_date"], op["resource_id"]),
        )
        current = cur.fetchone()
        if current and not event_should_apply(current.get("route_last_event_at"), current.get("route_last_event_fingerprint"), op["event_at"], op["fingerprint"]):
            return False
        route_state = op.get("route_state") or (current.get("route_state") if current else None) or "unknown"
        cur.execute(
            """
            INSERT INTO ofs_technician_operational_state
                (work_date,resource_id,route_state,route_state_raw,route_started_at,route_reactivated_at,route_ended_at,
                 route_last_event_at,route_last_event_type,route_last_event_fingerprint,calendar_start_at,calendar_end_at,resource_timezone,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                route_state=VALUES(route_state),route_state_raw=VALUES(route_state_raw),
                route_started_at=COALESCE(VALUES(route_started_at),route_started_at),
                route_reactivated_at=COALESCE(VALUES(route_reactivated_at),route_reactivated_at),
                route_ended_at=COALESCE(VALUES(route_ended_at),route_ended_at),
                route_last_event_at=VALUES(route_last_event_at),route_last_event_type=VALUES(route_last_event_type),
                route_last_event_fingerprint=VALUES(route_last_event_fingerprint),
                calendar_start_at=COALESCE(VALUES(calendar_start_at),calendar_start_at),
                calendar_end_at=COALESCE(VALUES(calendar_end_at),calendar_end_at),
                resource_timezone=COALESCE(VALUES(resource_timezone),resource_timezone),updated_at=VALUES(updated_at)
            """,
            (
                op["work_date"], op["resource_id"], route_state, op["event_type"], op.get("route_started_at"),
                op.get("route_reactivated_at"), op.get("route_ended_at"), op["event_at"], op["event_type"], op["fingerprint"],
                op.get("calendar_start_at"), op.get("calendar_end_at"), op.get("resource_timezone"), now,
            ),
        )
        return True

    def update_health(self, source: str, *, status: str, started_at: Optional[datetime] = None, success_at: Optional[datetime] = None, finished_at: Optional[datetime] = None, error_code: Optional[str] = None, error_message: Optional[str] = None) -> None:
        now = utc_now_naive()
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO ofs_operational_sync_state
                    (source_name,last_started_at,last_success_at,last_finished_at,status,error_code,error_message,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    last_started_at=COALESCE(VALUES(last_started_at),last_started_at),
                    last_success_at=COALESCE(VALUES(last_success_at),last_success_at),
                    last_finished_at=COALESCE(VALUES(last_finished_at),last_finished_at),
                    status=VALUES(status),error_code=VALUES(error_code),error_message=VALUES(error_message),updated_at=VALUES(updated_at)
                """,
                (source, started_at, success_at, finished_at, status, error_code, _safe_error_text(error_message), now),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def purge_retention(self, today: date, retention_days: int = 7) -> dict:
        cutoff = retention_cutoff(today, retention_days)
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM ofs_activity_operational_state WHERE work_date < %s OR work_date > %s", (cutoff, today))
            activities = max(int(cur.rowcount or 0), 0)
            cur.execute("DELETE FROM ofs_technician_operational_state WHERE work_date < %s OR work_date > %s", (cutoff, today))
            technicians = max(int(cur.rowcount or 0), 0)
            conn.commit()
            return {"cutoff": cutoff.isoformat(), "activities_deleted": activities, "technicians_deleted": technicians}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def operational_metrics(self, work_date: Optional[date] = None) -> dict:
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            params: Tuple[Any, ...] = ()
            tech_where = ""
            act_where = ""
            if work_date:
                tech_where = " WHERE work_date=%s"
                act_where = " WHERE work_date=%s"
                params = (work_date,)
            cur.execute(f"SELECT COUNT(*) AS total FROM ofs_technician_operational_state{tech_where}", params)
            tech_total = int((cur.fetchone() or {}).get("total") or 0)
            cur.execute(f"SELECT COUNT(*) AS total FROM ofs_activity_operational_state{act_where}", params)
            activity_total = int((cur.fetchone() or {}).get("total") or 0)
            cur.execute(
                """
                SELECT TABLE_NAME, DATA_LENGTH, INDEX_LENGTH
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME IN ('ofs_technician_operational_state','ofs_activity_operational_state','ofs_event_cursor','ofs_operational_sync_state')
                """
            )
            sizes = {
                row["TABLE_NAME"]: {
                    "data_bytes": int(row.get("DATA_LENGTH") or 0),
                    "index_bytes": int(row.get("INDEX_LENGTH") or 0),
                }
                for row in (cur.fetchall() or [])
            }
            return {"technician_rows": tech_total, "activity_rows": activity_total, "table_sizes": sizes}
        finally:
            cur.close()
            conn.close()


@contextmanager
def mysql_operational_lock(lock_name: str = LOCK_NAME, connection_factory: Optional[Callable] = None):
    connection_factory = connection_factory or _default_connection_factory
    conn = connection_factory()
    cur = conn.cursor()
    acquired = False
    try:
        cur.execute("SELECT GET_LOCK(%s,0)", (lock_name,))
        row = cur.fetchone()
        acquired = bool(row and row[0] == 1)
        if not acquired:
            raise OperationalAlreadyRunning("Já existe um worker operacional OFS em execução.")
        yield
    finally:
        try:
            if acquired:
                cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                cur.fetchone()
        finally:
            cur.close()
            conn.close()


class TechnicianOperationalCollector:
    def __init__(self, api: Optional[OFSOperationalAPI] = None, repository: Optional[MySQLOperationalRepository] = None, settings: Optional[OperationalSettings] = None, root_resource_id: Optional[str] = None):
        self.settings = settings or OperationalSettings.from_env()
        self.api = api or OFSOperationalAPI(settings=self.settings)
        self.repository = repository or MySQLOperationalRepository()
        self.root_resource_id = root_resource_id or get_ofs_root_resource_id()
        self._technicians: List[dict] = []
        self._technician_ids: set[str] = set()

    def refresh_technicians(self) -> List[dict]:
        self._technicians = self.repository.get_technicians(self.root_resource_id)
        self._technician_ids = {str(row["resource_id"]) for row in self._technicians}
        if not self._technicians:
            raise OperationalError("Nenhum técnico TCV/TCP/TCW encontrado na hierarquia local.")
        return self._technicians

    def ensure_subscription(self) -> dict:
        now = utc_now_naive()
        cursor = self.repository.get_cursor()
        if cursor and _clean(cursor.get("subscription_id")) and _clean(cursor.get("next_page")):
            existing = self.api.list_subscription(cursor["subscription_id"])
            if existing is not None:
                return {"created": False, "subscription_id": cursor["subscription_id"], "next_page": cursor["next_page"]}
        subscription_id, next_page = self.api.create_subscription()
        # Cursor inicial é persistido antes do baseline para fechar a janela de corrida.
        self.repository.save_new_subscription(subscription_id, next_page, now)
        return {"created": True, "subscription_id": subscription_id, "next_page": next_page}

    def reconcile_calendars(self, work_date: date) -> dict:
        cycle_started = time.perf_counter()
        started = utc_now_naive()
        self.repository.update_health("calendars", status="running", started_at=started)
        try:
            rows, calls = self.api.get_calendars(self.root_resource_id, work_date)
            applied = self.repository.apply_calendars(rows, self._technician_ids, utc_now_naive(), work_date)
            finished = utc_now_naive()
            self.repository.update_health("calendars", status="ok", success_at=finished, finished_at=finished)
            return {
                "api_calls": calls,
                "items": len(rows),
                "technicians_applied": applied,
                "elapsed_seconds": round(time.perf_counter() - cycle_started, 3),
            }
        except Exception as exc:
            finished = utc_now_naive()
            self.repository.update_health("calendars", status="error", finished_at=finished, error_code=exc.__class__.__name__, error_message=str(exc))
            raise

    def reconcile_activities(self, work_date: date) -> dict:
        cycle_started = time.perf_counter()
        started = utc_now_naive()
        self.repository.update_health("activities", status="running", started_at=started)
        try:
            rows, calls = self.api.get_activities(self.root_resource_id, work_date)
            marker = utc_now_naive()
            result = self.repository.replace_activities_for_date(work_date, rows, self._technician_ids, marker)
            finished = utc_now_naive()
            self.repository.update_health("activities", status="ok", success_at=finished, finished_at=finished)
            return {
                "api_calls": calls,
                "items": len(rows),
                "elapsed_seconds": round(time.perf_counter() - cycle_started, 3),
                **result,
            }
        except Exception as exc:
            finished = utc_now_naive()
            self.repository.update_health("activities", status="error", finished_at=finished, error_code=exc.__class__.__name__, error_message=str(exc))
            raise

    def baseline_routes(self, work_date: date) -> dict:
        cycle_started = time.perf_counter()
        started = utc_now_naive()
        self.repository.update_health("routes", status="running", started_at=started)
        results: List[dict] = []
        failures: List[Tuple[str, str]] = []
        calls = 0

        def fetch(technician: dict) -> Tuple[str, dict, int]:
            resource_id = str(technician["resource_id"])
            payload, route_calls = self.api.get_route(resource_id, work_date)
            return resource_id, payload, route_calls

        try:
            with ThreadPoolExecutor(max_workers=self.settings.route_workers) as executor:
                future_map = {executor.submit(fetch, technician): technician for technician in self._technicians}
                for future in as_completed(future_map):
                    resource_id = str(future_map[future]["resource_id"])
                    try:
                        rid, payload, route_calls = future.result()
                        calls += route_calls
                        results.append(normalize_route_baseline(rid, work_date, payload, reconciled_at=utc_now_naive()))
                    except Exception as exc:
                        failures.append((resource_id, _safe_error_text(exc)))
            if failures:
                raise OperationalError(f"Baseline Route parcial: {len(failures)} de {len(self._technicians)} técnicos falharam; amostra={failures[:3]}")
            self.repository.apply_route_baseline(results, utc_now_naive())
            finished = utc_now_naive()
            self.repository.update_health("routes", status="ok", success_at=finished, finished_at=finished)
            return {
                "api_calls": calls,
                "technicians": len(results),
                "failures": 0,
                "elapsed_seconds": round(time.perf_counter() - cycle_started, 3),
            }
        except Exception as exc:
            finished = utc_now_naive()
            self.repository.update_health("routes", status="error", finished_at=finished, error_code=exc.__class__.__name__, error_message=str(exc))
            raise

    def run_baseline(self, work_date: Optional[date] = None) -> dict:
        work_date = work_date or date.today()
        started = time.perf_counter()
        if not self._technicians:
            self.refresh_technicians()
        subscription = self.ensure_subscription()
        self.repository.ensure_technician_rows(self._technicians, work_date, utc_now_naive())
        calendars = self.reconcile_calendars(work_date)
        activities = self.reconcile_activities(work_date)
        routes = self.baseline_routes(work_date)
        completed_at = utc_now_naive()
        self.repository.mark_baseline_completed(completed_at)
        events = self.drain_events(max_pages=1000)
        retention = self.repository.purge_retention(work_date, self.settings.retention_days)
        return {
            "work_date": work_date.isoformat(),
            "subscription_created": subscription["created"],
            "technicians": len(self._technicians),
            "calendars": calendars,
            "activities": activities,
            "routes": routes,
            "retention": retention,
            "events_after_baseline": events,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }

    def poll_events_once(self) -> dict:
        if not self._technicians:
            self.refresh_technicians()
        cursor = self.repository.get_cursor()
        if not cursor:
            raise SubscriptionRecoveryRequired("Cursor inexistente")
        subscription_id = _clean(cursor.get("subscription_id"))
        page = _clean(cursor.get("next_page"))
        if not subscription_id or not page:
            raise SubscriptionRecoveryRequired("Cursor incompleto")
        started = utc_now_naive()
        self.repository.update_health("events", status="running", started_at=started)
        try:
            items, next_page = self.api.get_events(subscription_id, page)
            result = self.repository.apply_event_page(
                subscription_id, page, next_page, items, utc_now_naive(), self._technician_ids,
                today=date.today(), retention_days=self.settings.retention_days,
            )
            finished = utc_now_naive()
            self.repository.update_health("events", status="ok", success_at=finished, finished_at=finished)
            return {"page": page, "next_page": next_page, **result}
        except OperationalAPIError as exc:
            finished = utc_now_naive()
            self.repository.update_health("events", status="error", finished_at=finished, error_code=f"HTTP_{exc.status_code}" if exc.status_code else exc.__class__.__name__, error_message=str(exc))
            if exc.status_code in {400, 404, 410}:
                raise SubscriptionRecoveryRequired(str(exc)) from exc
            raise
        except Exception as exc:
            finished = utc_now_naive()
            self.repository.update_health("events", status="error", finished_at=finished, error_code=exc.__class__.__name__, error_message=str(exc))
            raise

    def drain_events(self, max_pages: int = 1000) -> dict:
        cycle_started = time.perf_counter()
        total_events = total_applied = total_unsafe = polls = 0
        for _ in range(max_pages):
            result = self.poll_events_once()
            polls += 1
            total_events += result["events"]
            total_applied += result["applied"]
            total_unsafe += result["unsafe"]
            if not result["page_changed"]:
                return {
                    "polls": polls,
                    "events": total_events,
                    "applied": total_applied,
                    "unsafe": total_unsafe,
                    "caught_up": True,
                    "elapsed_seconds": round(time.perf_counter() - cycle_started, 3),
                }
        return {
            "polls": polls,
            "events": total_events,
            "applied": total_applied,
            "unsafe": total_unsafe,
            "caught_up": False,
            "elapsed_seconds": round(time.perf_counter() - cycle_started, 3),
        }

    def run_forever(self, *, stop_predicate: Optional[Callable[[], bool]] = None) -> None:
        stop_predicate = stop_predicate or (lambda: False)
        self.refresh_technicians()
        current_date = date.today()
        LOGGER.info(
            "Worker operacional iniciado date=%s technicians=%s events_poll=%ss activities_reconcile=%ss calendars_reconcile=%ss retention_days=%s route_workers=%s",
            current_date.isoformat(),
            len(self._technicians),
            self.settings.events_poll_seconds,
            self.settings.activities_reconcile_seconds,
            self.settings.calendars_reconcile_seconds,
            self.settings.retention_days,
            self.settings.route_workers,
        )
        baseline = self.run_baseline(current_date)
        LOGGER.info(
            "Baseline operacional concluído date=%s technicians=%s elapsed=%ss route_calls=%s events=%s events_caught_up=%s subscription_created=%s",
            current_date.isoformat(),
            baseline.get("technicians"),
            baseline.get("elapsed_seconds"),
            (baseline.get("routes") or {}).get("api_calls"),
            (baseline.get("events_after_baseline") or {}).get("events"),
            (baseline.get("events_after_baseline") or {}).get("caught_up"),
            baseline.get("subscription_created"),
        )
        next_activities = time.monotonic() + self.settings.activities_reconcile_seconds
        next_calendars = time.monotonic() + self.settings.calendars_reconcile_seconds

        while not stop_predicate():
            now_date = date.today()
            if now_date != current_date:
                previous_date = current_date
                current_date = now_date
                LOGGER.info("Virada de dia operacional detectada previous_date=%s new_date=%s", previous_date.isoformat(), current_date.isoformat())
                self.refresh_technicians()
                baseline = self.run_baseline(current_date)
                LOGGER.info(
                    "Baseline de virada concluído date=%s technicians=%s elapsed=%ss route_calls=%s events=%s events_caught_up=%s",
                    current_date.isoformat(),
                    baseline.get("technicians"),
                    baseline.get("elapsed_seconds"),
                    (baseline.get("routes") or {}).get("api_calls"),
                    (baseline.get("events_after_baseline") or {}).get("events"),
                    (baseline.get("events_after_baseline") or {}).get("caught_up"),
                )
                next_activities = time.monotonic() + self.settings.activities_reconcile_seconds
                next_calendars = time.monotonic() + self.settings.calendars_reconcile_seconds
                continue

            try:
                result = self.drain_events()
                if not result["caught_up"]:
                    LOGGER.warning("Events backlog não drenado dentro do limite defensivo")
                elif result.get("events") or result.get("unsafe"):
                    LOGGER.info(
                        "Events reconciliado polls=%s events=%s applied=%s unsafe=%s elapsed=%ss caught_up=%s",
                        result.get("polls"),
                        result.get("events"),
                        result.get("applied"),
                        result.get("unsafe"),
                        result.get("elapsed_seconds"),
                        result.get("caught_up"),
                    )
                else:
                    LOGGER.debug("Events em dia polls=%s elapsed=%ss", result.get("polls"), result.get("elapsed_seconds"))
            except SubscriptionRecoveryRequired:
                LOGGER.warning("Subscription/cursor requer recuperação; recriando baseline")
                self.repository.save_new_subscription(*self.api.create_subscription(), utc_now_naive())
                baseline = self.run_baseline(current_date)
                LOGGER.info(
                    "Recuperação de subscription concluída date=%s technicians=%s elapsed=%ss route_calls=%s events=%s events_caught_up=%s",
                    current_date.isoformat(),
                    baseline.get("technicians"),
                    baseline.get("elapsed_seconds"),
                    (baseline.get("routes") or {}).get("api_calls"),
                    (baseline.get("events_after_baseline") or {}).get("events"),
                    (baseline.get("events_after_baseline") or {}).get("caught_up"),
                )
                continue
            except Exception as exc:
                LOGGER.error("Falha no ciclo Events: %s", _safe_error_text(exc))

            monotonic = time.monotonic()
            if monotonic >= next_activities:
                try:
                    activities = self.reconcile_activities(current_date)
                    LOGGER.info(
                        "Activities reconciliado date=%s api_calls=%s items=%s applied=%s elapsed=%ss",
                        current_date.isoformat(),
                        activities.get("api_calls"),
                        activities.get("items"),
                        activities.get("activities"),
                        activities.get("elapsed_seconds"),
                    )
                except Exception as exc:
                    LOGGER.error("Falha reconciliação Activities: %s", _safe_error_text(exc))
                next_activities = monotonic + self.settings.activities_reconcile_seconds
            if monotonic >= next_calendars:
                try:
                    calendars = self.reconcile_calendars(current_date)
                    LOGGER.info(
                        "Calendars reconciliado date=%s api_calls=%s items=%s technicians_applied=%s elapsed=%ss",
                        current_date.isoformat(),
                        calendars.get("api_calls"),
                        calendars.get("items"),
                        calendars.get("technicians_applied"),
                        calendars.get("elapsed_seconds"),
                    )
                except Exception as exc:
                    LOGGER.error("Falha reconciliação Calendars: %s", _safe_error_text(exc))
                next_calendars = monotonic + self.settings.calendars_reconcile_seconds

            retention = self.repository.purge_retention(current_date, self.settings.retention_days)
            if retention.get("activities_deleted") or retention.get("technicians_deleted"):
                LOGGER.info(
                    "Retenção operacional aplicada cutoff=%s activities_deleted=%s technicians_deleted=%s",
                    retention.get("cutoff"),
                    retention.get("activities_deleted"),
                    retention.get("technicians_deleted"),
                )
            time.sleep(self.settings.events_poll_seconds)
