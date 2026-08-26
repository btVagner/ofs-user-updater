import argparse
import json
import math
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from ofs.client import OFSClient  # noqa: E402
from ofs.config import get_ofs_root_resource_id  # noqa: E402


TECHNICIAN_RESOURCE_TYPES = ("TCV", "TCP", "TCW")
ROUTE_EVENTS = (
    "routeCreated",
    "routeUpdated",
    "routeActivated",
    "routeDeactivated",
    "routeReactivated",
)
ACTIVITY_EVENTS = (
    "activityStarted",
    "activityTravelStarted",
    "activityTravelStopped",
    "activitySuspended",
    "activityCompleted",
    "activityNotDone",
    "activityCanceled",
    "activityMoved",
)
REQUESTED_EVENTS = ROUTE_EVENTS + ACTIVITY_EVENTS
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
ROUTE_EVENT_FIELDS = (
    "date",
    "resourceId",
    "calendarTimeFrom",
    "calendarTimeTo",
    "timeZone",
    "activated",
    "deactivated",
)
SENSITIVE_KEY_RE = re.compile(
    r"(?i)(authorization|client[_-]?secret|password|passwd|access[_-]?token|refresh[_-]?token|bearer)"
)
SENSITIVE_INLINE_RE = re.compile(
    r"(?i)(authorization|client[_-]?secret|password|passwd|access[_-]?token|refresh[_-]?token)\s*[:=]\s*[^\s,;]+"
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _redact_text(value: Any, limit: int = 1200) -> str:
    text = str(value or "")[:limit]
    text = SENSITIVE_INLINE_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    return text


def _safe_endpoint(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        return parsed.path or "/"
    except Exception:
        return None


def _sanitize_error(exc: Exception, method: Optional[str] = None, endpoint: Optional[str] = None) -> dict:
    result = {
        "type": exc.__class__.__name__,
        "message": _redact_text(exc),
    }
    if method:
        result["method"] = method
    if endpoint:
        result["endpoint"] = _safe_endpoint(endpoint) or endpoint

    response = getattr(exc, "response", None)
    if response is not None:
        result["http_status"] = getattr(response, "status_code", None)
        result["endpoint"] = _safe_endpoint(getattr(response, "url", None)) or result.get("endpoint")
        try:
            body = response.json()
        except Exception:
            body = getattr(response, "text", "")
        result["response"] = _sanitize_json(body, max_depth=3)
    return result


def _sanitize_json(value: Any, *, max_depth: int = 4, _depth: int = 0) -> Any:
    if _depth >= max_depth:
        return "<truncated>"
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            key_text = str(key)
            if SENSITIVE_KEY_RE.search(key_text):
                sanitized[key_text] = "<redacted>"
            else:
                sanitized[key_text] = _sanitize_json(item, max_depth=max_depth, _depth=_depth + 1)
        return sanitized
    if isinstance(value, list):
        return [
            _sanitize_json(item, max_depth=max_depth, _depth=_depth + 1)
            for item in value[:20]
        ]
    if isinstance(value, str):
        return _redact_text(value, limit=500)
    return value


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> Tuple[requests.Response, Any]:
    response = session.request(
        method,
        url,
        params=params,
        json=json_body,
        timeout=timeout,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    if not response.ok:
        response.raise_for_status()
    if response.status_code == 204 or not response.content:
        return response, None
    return response, response.json()



def extract_supported_events_from_metadata(payload: Any) -> List[str]:
    documented = set(REQUESTED_EVENTS)
    found: Set[str] = set()

    def visit(value: Any):
        if isinstance(value, dict):
            enum_values = value.get("enum")
            if isinstance(enum_values, list):
                for item in enum_values:
                    if isinstance(item, str) and item in documented:
                        found.add(item)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return sorted(found)


def build_subscription_payload(title: str) -> dict:
    return {
        "subscriptionTitle": title,
        "subscriptionConfig": [
            {"events": list(ROUTE_EVENTS)},
            {
                "events": list(ACTIVITY_EVENTS),
                "fields": list(ACTIVITY_FIELDS),
            },
        ],
    }


def extract_subscription_ids(payload: Any) -> Set[str]:
    ids: Set[str] = set()
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    subscription_id = _clean(item.get("subscriptionId"))
                    if subscription_id:
                        ids.add(subscription_id)
    return ids


def subscription_fingerprint(subscription_id: Optional[str]) -> Optional[str]:
    if not subscription_id:
        return None
    text = str(subscription_id)
    if len(text) <= 8:
        return f"len={len(text)}"
    return f"len={len(text)},suffix={text[-6:]}"


def extract_route_event_observations(items: Any) -> dict:
    event_types = Counter()
    route_samples: List[dict] = []
    activity_samples: List[dict] = []
    route_fields_seen: Set[str] = set()
    calendar_values_seen = {"calendarTimeFrom": [], "calendarTimeTo": [], "timeZone": []}

    if not isinstance(items, list):
        items = []

    for event in items[:1000]:
        if not isinstance(event, dict):
            continue
        event_type = _clean(event.get("eventType")) or "<unknown>"
        event_types[event_type] += 1

        route_changes = event.get("routeChanges")
        if isinstance(route_changes, dict):
            route_fields_seen.update(str(key) for key in route_changes.keys())
            sample = {"eventType": event_type, "time": event.get("time")}
            for field in ROUTE_EVENT_FIELDS:
                if field in route_changes:
                    sample[field] = route_changes.get(field)
                    if field in calendar_values_seen and route_changes.get(field) is not None:
                        calendar_values_seen[field].append(route_changes.get(field))
            if len(route_samples) < 5:
                route_samples.append(sample)

        activity_details = event.get("activityDetails")
        activity_changes = event.get("activityChanges")
        if isinstance(activity_details, dict) or isinstance(activity_changes, dict):
            sample = {"eventType": event_type, "time": event.get("time")}
            if isinstance(activity_details, dict):
                sample["activityDetails"] = {
                    field: activity_details.get(field)
                    for field in ACTIVITY_FIELDS
                    if field in activity_details
                }
            if isinstance(activity_changes, dict):
                sample["activityChanges"] = {
                    field: activity_changes.get(field)
                    for field in ACTIVITY_FIELDS
                    if field in activity_changes
                }
            if len(activity_samples) < 5:
                activity_samples.append(sample)

    return {
        "event_type_counts": dict(sorted(event_types.items())),
        "route_event_fields_seen": sorted(route_fields_seen),
        "calendarTimeFrom_observed": calendar_values_seen["calendarTimeFrom"][:5],
        "calendarTimeTo_observed": calendar_values_seen["calendarTimeTo"][:5],
        "timezone_observed": calendar_values_seen["timeZone"][:5],
        "route_event_samples": _sanitize_json(route_samples, max_depth=5),
        "activity_event_samples": _sanitize_json(activity_samples, max_depth=5),
    }


def summarize_activity_items(items: Any) -> dict:
    if not isinstance(items, list):
        items = []
    status_counts = Counter()
    fields_seen: Set[str] = set()
    samples: List[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        fields_seen.update(str(key) for key in item.keys())
        status = _clean(item.get("status")) or "<missing>"
        status_counts[status] += 1
        if len(samples) < 5:
            samples.append(
                {
                    field: item.get(field)
                    for field in ACTIVITY_FIELDS
                    if field in item
                }
            )
    return {
        "items_count": len(items),
        "status_counts": dict(sorted(status_counts.items())),
        "fields_seen": sorted(fields_seen),
        "samples": _sanitize_json(samples, max_depth=4),
    }


def summarize_calendar_payload(payload: Any, probe_date: str) -> dict:
    if not isinstance(payload, dict):
        return {"date": probe_date, "present": False}
    item = payload.get(probe_date)
    if not isinstance(item, dict):
        return {"date": probe_date, "present": False, "top_level_keys": sorted(map(str, payload.keys()))}

    regular = item.get("regular") if isinstance(item.get("regular"), dict) else None
    on_call = item.get("on-call") if isinstance(item.get("on-call"), dict) else None
    return {
        "date": probe_date,
        "present": True,
        "regular": _sanitize_json(regular, max_depth=3),
        "on_call": _sanitize_json(on_call, max_depth=3),
    }


def estimate_api_volume(
    hierarchy_total: Optional[int],
    technician_total: Optional[int],
    *,
    calendar_page_size: int = 100,
    events_poll_seconds: int = 60,
) -> dict:
    result: Dict[str, Any] = {
        "calendar_page_size": calendar_page_size,
        "events_poll_seconds": events_poll_seconds,
        "events_get_calls_per_day_at_poll_interval": math.ceil(86400 / events_poll_seconds),
    }
    if hierarchy_total is not None:
        result["calendar_baseline_calls_upper_bound_one_day"] = math.ceil(
            max(hierarchy_total, 0) / calendar_page_size
        )
    if technician_total is not None:
        techs = max(technician_total, 0)
        result["route_baseline_minimum_calls_if_all_technicians"] = techs
        result["route_polling_calls_per_day_if_each_technician_every_minute"] = techs * 1440
    return result


def load_hierarchy_context(root_resource_id: str) -> dict:
    conn = None
    cursor = None
    try:
        from database.connection import get_connection

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT
                COUNT(*) AS hierarchy_total,
                SUM(CASE WHEN resource_type IN ('TCV','TCP','TCW') THEN 1 ELSE 0 END) AS technician_total,
                MAX(depth) AS max_depth
            FROM ofs_resource_hierarchy
            WHERE root_resource_id = %s
            """,
            (root_resource_id,),
        )
        stats = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT resource_id, resource_type, timezone, depth
            FROM ofs_resource_hierarchy
            WHERE root_resource_id = %s
              AND resource_type IN ('TCV','TCP','TCW')
            ORDER BY resource_id
            LIMIT 1
            """,
            (root_resource_id,),
        )
        sample = cursor.fetchone()

        cursor.execute(
            """
            SELECT resource_type, COUNT(*) AS total
            FROM ofs_resource_hierarchy
            WHERE root_resource_id = %s
            GROUP BY resource_type
            ORDER BY resource_type
            """,
            (root_resource_id,),
        )
        type_counts = {
            str(row["resource_type"] or "<null>"): int(row["total"])
            for row in (cursor.fetchall() or [])
        }

        return {
            "accessible": True,
            "root_resource_id": root_resource_id,
            "hierarchy_total": int(stats.get("hierarchy_total") or 0),
            "technician_total": int(stats.get("technician_total") or 0),
            "max_depth": int(stats.get("max_depth") or 0),
            "resource_type_counts": type_counts,
            "sample_technician": sample,
        }
    except Exception as exc:
        return {
            "accessible": False,
            "root_resource_id": root_resource_id,
            "error": _sanitize_error(exc),
        }
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _probe_get(
    report: dict,
    key: str,
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    params: Optional[dict] = None,
) -> Optional[Any]:
    started = time.perf_counter()
    try:
        response, payload = _request_json(
            session,
            "GET",
            url,
            timeout=timeout,
            params=params,
        )
        report[key] = {
            "accessible": True,
            "http_status": response.status_code,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
        }
        return payload
    except Exception as exc:
        report[key] = {
            "accessible": False,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
            "error": _sanitize_error(exc, method="GET", endpoint=url),
        }
        return None


def run_probe(args) -> dict:
    root_resource_id = args.root or get_ofs_root_resource_id()
    probe_date = args.date or date.today().isoformat()
    report: Dict[str, Any] = {
        "probe": "ofs_operational_api_probe",
        "generated_at_utc": _utc_now_iso(),
        "root_resource_id": root_resource_id,
        "probe_date": probe_date,
        "requested_events": list(REQUESTED_EVENTS),
        "documented_route_event_fields": list(ROUTE_EVENT_FIELDS),
        "requested_activity_fields": list(ACTIVITY_FIELDS),
        "safety": {
            "prints_credentials": False,
            "prints_authorization_header": False,
            "subscription_cleanup_policy": (
                "delete only when GET subscriptions proved the returned subscriptionId did not exist before the probe"
            ),
        },
    }

    hierarchy = load_hierarchy_context(root_resource_id)
    report["hierarchy"] = hierarchy
    hierarchy_total = hierarchy.get("hierarchy_total") if hierarchy.get("accessible") else None
    technician_total = hierarchy.get("technician_total") if hierarchy.get("accessible") else None
    report["performance_estimate"] = estimate_api_volume(
        hierarchy_total,
        technician_total,
        events_poll_seconds=args.events_poll_seconds,
    )

    client = OFSClient()
    if not _clean(client.username) or not _clean(client.password):
        report["configuration_error"] = (
            "OFS_USERNAME/OFS_PASSWORD não estão configurados no ambiente. Nenhum valor foi exibido."
        )
        return report

    resource_id = _clean(args.resource_id)
    if not resource_id:
        sample = hierarchy.get("sample_technician") if hierarchy.get("accessible") else None
        if isinstance(sample, dict):
            resource_id = _clean(sample.get("resource_id"))

    if not resource_id:
        report["configuration_error"] = (
            "Não foi possível escolher recurso de teste. Informe --resource-id ou disponibilize a tabela ofs_resource_hierarchy."
        )
        return report

    report["resource_id"] = resource_id

    session = requests.Session()
    session.auth = client.auth
    base_url = client.base_url.rstrip("/")

    # Recurso e timezone cadastral.
    resource_payload = _probe_get(
        report,
        "resource_get",
        session,
        f"{base_url}/resources/{resource_id}",
        timeout=args.timeout,
        params={"fields": "resourceId,resourceType,status,timeZone,timeZoneDiff"},
    )
    if isinstance(resource_payload, dict):
        report["resource_get"]["fields"] = {
            key: resource_payload.get(key)
            for key in ("resourceId", "resourceType", "status", "timeZone", "timeZoneDiff")
            if key in resource_payload
        }

    # Jornada individual / calendarView.
    calendar_payload = _probe_get(
        report,
        "calendar_get",
        session,
        f"{base_url}/resources/{resource_id}/workSchedules/calendarView",
        timeout=args.timeout,
        params={"dateFrom": probe_date, "dateTo": probe_date},
    )
    if calendar_payload is not None:
        report["calendar_get"]["calendar"] = summarize_calendar_payload(calendar_payload, probe_date)

    # Confirma a API de calendários em lote sem varrer tudo durante o probe.
    bulk_calendar_payload = _probe_get(
        report,
        "bulk_calendar_get",
        session,
        f"{base_url}/calendars",
        timeout=args.timeout,
        params={
            "resources": root_resource_id,
            "dateFrom": probe_date,
            "dateTo": probe_date,
            "includeChildren": "all",
            "includeInactive": "false",
            "limit": min(max(args.calendar_probe_limit, 1), 100),
            "offset": 0,
        },
    )
    if isinstance(bulk_calendar_payload, dict):
        items = bulk_calendar_payload.get("items") or []
        report["bulk_calendar_get"]["items_returned"] = len(items) if isinstance(items, list) else 0
        report["bulk_calendar_get"]["sample_keys"] = (
            sorted(map(str, items[0].keys())) if isinstance(items, list) and items and isinstance(items[0], dict) else []
        )

    # Rota diária do técnico, paginada com máximo oficial de 100 itens.
    route_items: List[dict] = []
    route_first: Optional[dict] = None
    route_calls = 0
    offset = 0
    route_started = time.perf_counter()
    try:
        while True:
            response, payload = _request_json(
                session,
                "GET",
                f"{base_url}/resources/{resource_id}/routes/{probe_date}",
                timeout=args.timeout,
                params={
                    "activityFields": ",".join(ACTIVITY_FIELDS),
                    "limit": 100,
                    "offset": offset,
                },
            )
            route_calls += 1
            if not isinstance(payload, dict):
                raise ValueError("Resposta de rota não é um objeto JSON.")
            if route_first is None:
                route_first = payload
            page_items = payload.get("items") or []
            if not isinstance(page_items, list):
                raise ValueError("Campo items da rota não é uma lista.")
            route_items.extend(item for item in page_items if isinstance(item, dict))
            total_results = int(payload.get("totalResults") or len(route_items))
            if not page_items or len(route_items) >= total_results:
                break
            offset += len(page_items)
            if route_calls >= args.max_route_pages:
                break

        report["route_get"] = {
            "accessible": True,
            "http_status": response.status_code,
            "elapsed_ms": round((time.perf_counter() - route_started) * 1000, 2),
            "api_calls": route_calls,
            "total_results": int((route_first or {}).get("totalResults") or len(route_items)),
            "routeStartTime": (route_first or {}).get("routeStartTime"),
            "routeReactivationTime": (route_first or {}).get("routeReactivationTime"),
            "routeEndTime": (route_first or {}).get("routeEndTime"),
            "activities": summarize_activity_items(route_items),
        }
    except Exception as exc:
        report["route_get"] = {
            "accessible": False,
            "elapsed_ms": round((time.perf_counter() - route_started) * 1000, 2),
            "api_calls": route_calls,
            "error": _sanitize_error(
                exc,
                method="GET",
                endpoint=f"{base_url}/resources/{resource_id}/routes/{probe_date}",
            ),
        }

    # Activities API separada, limitada ao recurso de teste.
    activities_payload = _probe_get(
        report,
        "activities_get",
        session,
        f"{base_url}/activities",
        timeout=args.timeout,
        params={
            "resources": resource_id,
            "includeChildren": "none",
            "dateFrom": probe_date,
            "dateTo": probe_date,
            "fields": ",".join(ACTIVITY_FIELDS),
            "limit": min(max(args.activity_probe_limit, 1), 100),
            "offset": 0,
        },
    )
    if isinstance(activities_payload, dict):
        items = activities_payload.get("items") or []
        report["activities_get"]["total_results"] = activities_payload.get("totalResults")
        report["activities_get"]["has_more"] = activities_payload.get("hasMore")
        report["activities_get"]["activities"] = summarize_activity_items(items)

    # Metadata dos eventos: ajuda a diferenciar documentação de suporte efetivo no tenant.
    metadata_payload = _probe_get(
        report,
        "events_metadata_get",
        session,
        f"{base_url}/metadata-catalog/events",
        timeout=args.timeout,
    )
    if metadata_payload is not None:
        supported = extract_supported_events_from_metadata(metadata_payload)
        report["events_metadata_get"]["requested_events_found_in_metadata"] = supported
        report["events_metadata_get"]["all_requested_events_found"] = set(supported) == set(REQUESTED_EVENTS)

    # Lista antes da criação: necessária para não apagar subscription preexistente se a API reutilizar duplicata.
    subscriptions_payload = _probe_get(
        report,
        "subscriptions_get_before",
        session,
        f"{base_url}/events/subscriptions",
        timeout=args.timeout,
    )
    preexisting_ids: Optional[Set[str]] = None
    if subscriptions_payload is not None:
        preexisting_ids = extract_subscription_ids(subscriptions_payload)
        report["subscriptions_get_before"]["subscription_count"] = len(preexisting_ids)

    subscription_id: Optional[str] = None
    next_page: Optional[str] = None
    create_started = time.perf_counter()
    try:
        title = f"ofs_d05_probe_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        response, payload = _request_json(
            session,
            "POST",
            f"{base_url}/events/subscriptions",
            timeout=args.timeout,
            json_body=build_subscription_payload(title),
        )
        if not isinstance(payload, dict):
            raise ValueError("Resposta de criação da subscription não é objeto JSON.")
        subscription_id = _clean(payload.get("subscriptionId"))
        next_page = _clean(payload.get("nextPage"))
        if not subscription_id or not next_page:
            raise ValueError("Resposta de subscription sem subscriptionId/nextPage.")

        existed_before = subscription_id in preexisting_ids if preexisting_ids is not None else None
        report["subscription_create"] = {
            "created_or_reused": True,
            "http_status": response.status_code,
            "elapsed_ms": round((time.perf_counter() - create_started) * 1000, 2),
            "subscription_fingerprint": subscription_fingerprint(subscription_id),
            "nextPage": next_page,
            "nextPage_length": len(next_page),
            "existed_before_probe": existed_before,
            "requested_events": list(REQUESTED_EVENTS),
        }
    except Exception as exc:
        report["subscription_create"] = {
            "created_or_reused": False,
            "elapsed_ms": round((time.perf_counter() - create_started) * 1000, 2),
            "error": _sanitize_error(
                exc,
                method="POST",
                endpoint=f"{base_url}/events/subscriptions",
            ),
        }

    # Leitura dos eventos a partir do cursor devolvido pela criação.
    if subscription_id and next_page:
        event_items: List[dict] = []
        page = next_page
        pages_seen: List[dict] = []
        events_started = time.perf_counter()
        try:
            for poll_index in range(max(args.event_polls, 1)):
                response, payload = _request_json(
                    session,
                    "GET",
                    f"{base_url}/events",
                    timeout=args.timeout,
                    params={
                        "subscriptionId": subscription_id,
                        "page": page,
                        "limit": min(max(args.event_limit, 1), 1000),
                    },
                )
                if not isinstance(payload, dict):
                    raise ValueError("Resposta de eventos não é objeto JSON.")
                items = payload.get("items") or []
                if not isinstance(items, list):
                    raise ValueError("Campo items de eventos não é lista.")
                new_next_page = _clean(payload.get("nextPage")) or page
                event_items.extend(item for item in items if isinstance(item, dict))
                pages_seen.append(
                    {
                        "poll": poll_index + 1,
                        "items": len(items),
                        "page_changed": new_next_page != page,
                        "nextPage_length": len(new_next_page),
                    }
                )
                page_changed = new_next_page != page
                page = new_next_page

                if page_changed:
                    # Há backlog/cursor avançando: continue sem atraso.
                    continue
                if poll_index + 1 < max(args.event_polls, 1) and args.event_poll_interval > 0:
                    time.sleep(args.event_poll_interval)

            report["events_get"] = {
                "accessible": True,
                "http_status": response.status_code,
                "elapsed_ms": round((time.perf_counter() - events_started) * 1000, 2),
                "items_total": len(event_items),
                "polls": pages_seen,
                "final_nextPage": page,
                "observations": extract_route_event_observations(event_items),
            }
        except Exception as exc:
            report["events_get"] = {
                "accessible": False,
                "elapsed_ms": round((time.perf_counter() - events_started) * 1000, 2),
                "error": _sanitize_error(exc, method="GET", endpoint=f"{base_url}/events"),
            }

        # Cleanup seguro: nunca apagar uma subscription que já existia antes do probe.
        existed_before = (
            subscription_id in preexisting_ids if preexisting_ids is not None else None
        )
        if args.keep_subscription:
            report["subscription_cleanup"] = {
                "attempted": False,
                "reason": "--keep-subscription informado",
                "automatic_expiry_note": "subscription sem leitura expira automaticamente após aproximadamente 36 horas",
            }
        elif existed_before is True:
            report["subscription_cleanup"] = {
                "attempted": False,
                "reason": "subscription já existia antes do probe; exclusão seria insegura",
            }
        elif existed_before is None:
            report["subscription_cleanup"] = {
                "attempted": False,
                "reason": "não foi possível listar subscriptions antes da criação; não há prova segura de que o ID seja novo",
                "automatic_expiry_note": "subscription sem leitura expira automaticamente após aproximadamente 36 horas",
            }
        else:
            cleanup_started = time.perf_counter()
            try:
                response, _ = _request_json(
                    session,
                    "DELETE",
                    f"{base_url}/events/subscriptions/{subscription_id}",
                    timeout=args.timeout,
                )
                report["subscription_cleanup"] = {
                    "attempted": True,
                    "deleted": True,
                    "http_status": response.status_code,
                    "elapsed_ms": round((time.perf_counter() - cleanup_started) * 1000, 2),
                }
            except Exception as exc:
                report["subscription_cleanup"] = {
                    "attempted": True,
                    "deleted": False,
                    "elapsed_ms": round((time.perf_counter() - cleanup_started) * 1000, 2),
                    "error": _sanitize_error(
                        exc,
                        method="DELETE",
                        endpoint=f"{base_url}/events/subscriptions/{subscription_id}",
                    ),
                    "automatic_expiry_note": "se a subscription for nova e ficar sem leitura, expira automaticamente após aproximadamente 36 horas",
                }

    report["events_api_accessible"] = bool(
        report.get("subscriptions_get_before", {}).get("accessible")
        or report.get("events_metadata_get", {}).get("accessible")
        or report.get("events_get", {}).get("accessible")
    )
    report["subscription_creation_accessible"] = bool(
        report.get("subscription_create", {}).get("created_or_reused")
    )
    return report


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Valida de forma controlada APIs operacionais do Oracle Field Service sem imprimir credenciais."
        )
    )
    parser.add_argument("--resource-id", default=None, help="Técnico/recurso de teste. Se omitido, tenta escolher um TCV/TCP/TCW da hierarquia local.")
    parser.add_argument("--root", default=None, help="Root OFS. Padrão: configuração central OFS_ROOT_RESOURCE_ID.")
    parser.add_argument("--date", default=None, help="Data YYYY-MM-DD. Padrão: data local do host.")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout HTTP por chamada em segundos.")
    parser.add_argument("--event-limit", type=int, default=100, help="Máximo de eventos por GET (1..1000).")
    parser.add_argument("--event-polls", type=int, default=3, help="Quantidade de leituras do cursor de Events API.")
    parser.add_argument("--event-poll-interval", type=float, default=2.0, help="Intervalo entre leituras quando o cursor não avança.")
    parser.add_argument("--events-poll-seconds", type=int, default=60, help="Cadência usada apenas para estimativa de volume diário.")
    parser.add_argument("--activity-probe-limit", type=int, default=20, help="Quantidade de atividades do recurso de teste na chamada diagnóstica.")
    parser.add_argument("--calendar-probe-limit", type=int, default=20, help="Quantidade de itens da primeira página da API bulk de calendários.")
    parser.add_argument("--max-route-pages", type=int, default=10, help="Limite defensivo de páginas da rota no probe.")
    parser.add_argument("--keep-subscription", action="store_true", help="Não excluir uma subscription nova criada pelo probe.")
    parser.add_argument("--output", default=None, help="Arquivo JSON seguro opcional. Se omitido, somente stdout.")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.events_poll_seconds < 1:
        print("ERRO: --events-poll-seconds deve ser >= 1", file=sys.stderr)
        return 2
    if args.timeout < 1:
        print("ERRO: --timeout deve ser >= 1", file=sys.stderr)
        return 2

    report = run_probe(args)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = ROOT_DIR / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OFS_OPERATIONAL_API_PROBE] relatório salvo em: {output_path}", file=sys.stderr)

    return 0 if "configuration_error" not in report else 2


if __name__ == "__main__":
    raise SystemExit(main())
