from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from services.ofs_technician_operational_service import sanitize_operational_error


PERMISSION = "ofs.monitor_operacional"
SNAPSHOT_TTL_SECONDS = 10 * 60
SNAPSHOT_REFRESH_STALE_SECONDS = 2 * 60
SNAPSHOT_LOCK_NAME = "ofs_operational_monitor_snapshot_refresh"
TECHNICIAN_RESOURCE_TYPES = {"TCV", "TCP", "TCW"}
ACTIVE_OS_STATUSES = {"pending", "started", "enroute"}
NON_OS_TYPES = {"LUNCH", "ALM", "MAN_VEIC", "REUNIAO", "EQP_DUP"}
NON_OS_RECORD_TYPES = {"lunch", "break", "travel"}
SUPPORTED_SLOTS = {
    "08:00-12:00": "Manhã",
    "13:00-19:00": "Tarde",
    "18:00-21:00": "Noite",
    "13:00-21:00": "Tarde/noite",
}
SOURCE_FRESHNESS_SECONDS = {"events": 5 * 60, "activities": 20 * 60, "calendars": 90 * 60}


class MonitorError(RuntimeError):
    pass


class MonitorScopeError(MonitorError):
    pass


class MonitorRefreshInProgress(MonitorError):
    pass


class MonitorRefreshCooldown(MonitorError):
    def __init__(self, snapshot: dict):
        super().__init__("O snapshot ainda está dentro da janela compartilhada de 10 minutos.")
        self.snapshot = snapshot


class MonitorSourceUnavailable(MonitorError):
    pass


@dataclass(frozen=True)
class SourceSnapshot:
    hierarchy: List[dict]
    technician_states: List[dict]
    activities: List[dict]
    health: Dict[str, dict]


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def operational_work_date(now_utc: Optional[datetime] = None) -> date:
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    zone_name = (os.getenv("OFS_MONITOR_DEFAULT_TIMEZONE") or "America/Sao_Paulo").strip()
    return now_utc.astimezone(_safe_zone(zone_name)).date()


def monitor_scopes() -> Dict[str, dict]:
    return {
        "casa-cliente": {
            "key": "casa-cliente",
            "label": "Casa e Cliente",
            "root_resource_id": (os.getenv("OFS_MONITOR_CASA_CLIENTE_ROOT_ID") or "02").strip(),
        }
    }


def resolve_scope(scope_key: str) -> dict:
    key = str(scope_key or "").strip().lower()
    scope = monitor_scopes().get(key)
    if not scope:
        raise MonitorScopeError("Macro operacional inválida.")
    return dict(scope)


def _json_default(value: Any):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Tipo não serializável: {type(value)!r}")


def _dt_text(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat(timespec="seconds") if isinstance(value, datetime) else None


def _safe_zone(*values: Any) -> ZoneInfo:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            return ZoneInfo(text)
        except (ZoneInfoNotFoundError, ValueError):
            continue
    return ZoneInfo("America/Sao_Paulo")


def _is_real_os(row: Mapping[str, Any]) -> bool:
    activity_type = str(row.get("activity_type") or "").strip().upper()
    record_type = str(row.get("record_type") or "").strip().lower()
    return activity_type not in NON_OS_TYPES and record_type not in NON_OS_RECORD_TYPES


def _is_withdrawal(row: Mapping[str, Any]) -> bool:
    activity_type = str(row.get("activity_type") or "").strip().upper()
    return bool(re.match(r"^(?:RET(?:$|[_\-\s])|RETIRAD(?:A|AS)(?:$|[_\-\s]))", activity_type))


def _route_state(row: Mapping[str, Any]) -> str:
    starts = [value for value in (row.get("route_started_at"), row.get("route_reactivated_at")) if value]
    ended = row.get("route_ended_at")
    if starts:
        latest_start = max(starts)
        return "active" if ended is None or ended < latest_start else "ended"
    raw = str(row.get("route_state") or "").strip().lower()
    if raw in {"active", "started"}:
        return "active"
    if raw in {"ended", "closed", "deactivated"}:
        return "ended"
    return "not_started"


def _parse_slot(value: Any) -> Optional[dict]:
    match = re.match(r"^(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})$", str(value or "").strip())
    if not match:
        return None
    start_hour, start_minute, end_hour, end_minute = map(int, match.groups())
    if start_hour > 23 or end_hour > 23 or start_minute > 59 or end_minute > 59:
        return None
    key = f"{start_hour:02d}:{start_minute:02d}-{end_hour:02d}:{end_minute:02d}"
    if key not in SUPPORTED_SLOTS:
        return None
    return {
        "key": key,
        "label": SUPPORTED_SLOTS[key],
        "end": end_hour * 60 + end_minute,
        "end_text": f"{end_hour:02d}:{end_minute:02d}",
    }


def _minutes_since_midnight(value: datetime) -> float:
    return value.hour * 60 + value.minute + value.second / 60.0


def _local_epoch_ms(value: datetime, zone: ZoneInfo) -> int:
    localized = value.replace(tzinfo=zone) if value.tzinfo is None else value.astimezone(zone)
    return int(localized.timestamp() * 1000)


def _area_for(resource_id: str, hierarchy_by_id: Mapping[str, dict]) -> str:
    parts: List[str] = []
    visited = set()
    current = hierarchy_by_id.get(resource_id)
    while current:
        current_id = str(current.get("resource_id") or "")
        if not current_id or current_id in visited:
            break
        visited.add(current_id)
        parent_id = str(current.get("parent_resource_id") or "")
        parent = hierarchy_by_id.get(parent_id)
        if not parent:
            break
        if str(parent.get("resource_type") or "").upper() not in TECHNICIAN_RESOURCE_TYPES:
            parts.insert(0, str(parent.get("resource_name") or parent_id))
        current = parent
    return " / ".join(parts[-2:]) if parts else "Sem área"


def _customer_state(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    return normalized or "Sem UF"


def _common_activity(row: Mapping[str, Any], technician: Mapping[str, Any], area: str) -> dict:
    customer_state = _customer_state(row.get("customer_state"))
    return {
        "id": str(row.get("activity_id") or ""),
        "appt": str(row.get("appt_number") or ""),
        "tech": str(technician.get("resource_name") or row.get("resource_id") or "—"),
        "resource_id": str(row.get("resource_id") or ""),
        "area": area,
        "type": str(row.get("activity_type") or "—"),
        "start": _dt_text(row.get("start_time")) or "—",
        "status": str(row.get("status") or "—"),
        "time_slot": str(row.get("time_slot") or "—"),
        "customer": str(row.get("customer_name") or ""),
        "state": customer_state,
        "states": [customer_state],
    }


def build_monitor_payload(
    source: SourceSnapshot,
    scope: Mapping[str, Any],
    work_date: date,
    *,
    now: Optional[datetime] = None,
) -> dict:
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    hierarchy_by_id = {
        str(row.get("resource_id")): dict(row)
        for row in source.hierarchy
        if row.get("resource_id") is not None
    }
    technicians = {
        resource_id: row
        for resource_id, row in hierarchy_by_id.items()
        if str(row.get("resource_type") or "").upper() in TECHNICIAN_RESOURCE_TYPES
        and str(row.get("status") or "active").lower() == "active"
    }
    if not technicians:
        raise MonitorSourceUnavailable(
            f"A macro {scope['label']} ainda não possui técnicos sincronizados no read model local."
        )
    if not source.technician_states:
        raise MonitorSourceUnavailable(
            f"O estado operacional da macro {scope['label']} ainda não foi sincronizado para {work_date.isoformat()}."
        )

    state_by_id = {
        str(row.get("resource_id")): dict(row)
        for row in source.technician_states
        if row.get("resource_id") is not None
    }
    activities_by_resource: Dict[str, List[dict]] = {}
    for row in source.activities:
        resource_id = str(row.get("resource_id") or "")
        if resource_id in technicians:
            activities_by_resource.setdefault(resource_id, []).append(dict(row))

    result = {
        "schema_version": 2,
        "scope": dict(scope),
        "work_date": work_date.isoformat(),
        "generated_at": now_utc.isoformat(timespec="seconds"),
        "technicians_count": len(technicians),
        "late_candidates": [],
        "idle": [],
        "not_started_candidates": [],
        "slot": [],
        "black": [],
        "source_health": {},
        "diagnostics": {
            "activities_count": len(source.activities),
            "technicians_with_state": len(state_by_id),
            "missing_technician_state": max(len(technicians) - len(state_by_id), 0),
        },
    }

    source_usable = {}
    now_naive = now_utc.astimezone(timezone.utc).replace(tzinfo=None)
    for source_name, health in source.health.items():
        last_success_at = health.get("last_success_at")
        age_seconds = max(int((now_naive - last_success_at).total_seconds()), 0) if last_success_at else None
        threshold = SOURCE_FRESHNESS_SECONDS.get(source_name)
        usable = str(health.get("status") or "").lower() == "ok" and (
            threshold is None or (age_seconds is not None and age_seconds <= threshold)
        )
        source_usable[source_name] = usable
        result["source_health"][source_name] = {
            "status": health.get("status"),
            "last_success_at": _dt_text(last_success_at),
            "age_seconds": age_seconds,
            "fresh": usable,
            "error_code": health.get("error_code"),
        }
    result["diagnostics"]["data_complete"] = all(
        source_usable.get(name, False) for name in ("events", "activities", "calendars")
    ) and result["diagnostics"]["missing_technician_state"] == 0

    for resource_id, technician in technicians.items():
        state = state_by_id.get(resource_id) or {}
        zone = _safe_zone(state.get("resource_timezone_iana"), technician.get("timezone"))
        local_now = now_utc.astimezone(zone)
        if local_now.date() != work_date:
            continue
        area = _area_for(resource_id, hierarchy_by_id)
        resource_activities = activities_by_resource.get(resource_id, [])
        real_activities = [row for row in resource_activities if _is_real_os(row)]
        technician_states = sorted(
            {_customer_state(row.get("customer_state")) for row in real_activities},
            key=str.casefold,
        ) or ["Sem UF"]

        for activity in real_activities:
            common = _common_activity(activity, technician, area)
            status = str(activity.get("status") or "").lower()
            start_time = activity.get("start_time")
            duration = activity.get("duration_minutes")

            if bool(activity.get("is_black")):
                result["black"].append({**common, "black_value": 1})

            if status in ACTIVE_OS_STATUSES and start_time:
                slot = _parse_slot(activity.get("time_slot"))
                if slot and start_time.date() == work_date:
                    late_minutes = _minutes_since_midnight(start_time) - slot["end"]
                    if late_minutes >= 0:
                        result["slot"].append({
                            **common,
                            "slot_label": slot["label"],
                            "slot_key": slot["key"],
                            "slot_end": slot["end_text"],
                            "slot_late_minutes": round(late_minutes, 2),
                            "is_withdrawal": _is_withdrawal(activity),
                        })

            if status == "started" and start_time and duration and duration > 0:
                started_epoch = _local_epoch_ms(start_time, zone)
                if started_epoch <= int(now_utc.timestamp() * 1000):
                    result["late_candidates"].append({
                        **common,
                        "started_epoch_ms": started_epoch,
                        "duration_minutes": int(duration),
                    })

        state_name = _route_state(state)
        shift_start = state.get("calendar_start_at")
        shift_end = state.get("calendar_end_at")
        active_real_os = [
            row for row in real_activities
            if str(row.get("status") or "").lower() in ACTIVE_OS_STATUSES
        ]
        if state_name == "active" and not active_real_os and source_usable.get("events") and source_usable.get("activities"):
            if not shift_end or local_now.replace(tzinfo=None) < shift_end:
                route_start = state.get("route_reactivated_at") or state.get("route_started_at")
                result["idle"].append({
                    "tech": str(technician.get("resource_name") or resource_id),
                    "resource_id": resource_id,
                    "area": area,
                    "route_start": _dt_text(route_start) or "—",
                    "shift": (
                        f"{shift_start.strftime('%H:%M')}–{shift_end.strftime('%H:%M')}"
                        if shift_start and shift_end else "Não informado"
                    ),
                    "os_count": 0,
                    "situation": "Rota ativa · sem OS" if shift_end else "Rota ativa · jornada indisponível",
                    "states": technician_states,
                })
        elif (
            state_name == "not_started" and shift_start and shift_end
            and source_usable.get("events") and source_usable.get("calendars")
            and str((source.health.get("routes") or {}).get("status") or "").lower() == "ok"
        ):
            start_epoch = _local_epoch_ms(shift_start, zone)
            end_epoch = _local_epoch_ms(shift_end, zone)
            now_epoch = int(now_utc.timestamp() * 1000)
            if start_epoch <= now_epoch < end_epoch:
                result["not_started_candidates"].append({
                    "tech": str(technician.get("resource_name") or resource_id),
                    "resource_id": resource_id,
                    "area": area,
                    "shift": f"{shift_start.strftime('%H:%M')}–{shift_end.strftime('%H:%M')}",
                    "expected": shift_start.strftime("%H:%M"),
                    "shift_start_epoch_ms": start_epoch,
                    "shift_end_epoch_ms": end_epoch,
                    "situation": "Rota não ativada",
                    "states": technician_states,
                })

    result["idle"].sort(key=lambda row: row["tech"].casefold())
    result["slot"].sort(key=lambda row: row["slot_late_minutes"], reverse=True)
    result["black"].sort(key=lambda row: row["tech"].casefold())
    return result


class MySQLOperationalMonitorRepository:
    def __init__(self, connection_factory: Optional[Callable] = None):
        if connection_factory is None:
            from database.connection import get_connection

            connection_factory = get_connection
        self.connection_factory = connection_factory

    def load_snapshot(self, scope_key: str) -> Optional[dict]:
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT scope_key,work_date,status,payload_json,refreshed_at,expires_at,
                       refresh_started_at,refresh_finished_at,requested_by_username,error_text
                FROM ofs_operational_monitor_snapshot
                WHERE scope_key=%s
                """,
                (scope_key,),
            )
            return self._serialize_snapshot(cur.fetchone())
        finally:
            cur.close()
            conn.close()

    def load_source(self, conn, scope: Mapping[str, Any], work_date: date) -> SourceSnapshot:
        cur = conn.cursor(dictionary=True)
        root_resource_id = scope["root_resource_id"]
        try:
            cur.execute(
                """
                SELECT resource_id,parent_resource_id,resource_name,resource_type,status,timezone,depth
                FROM ofs_resource_hierarchy
                WHERE root_resource_id=%s
                ORDER BY depth,resource_id
                """,
                (root_resource_id,),
            )
            hierarchy = [dict(row) for row in (cur.fetchall() or [])]
            if not hierarchy:
                raise MonitorSourceUnavailable(
                    f"A hierarquia da macro {scope['label']} ({root_resource_id}) ainda não foi sincronizada."
                )

            cur.execute(
                """
                SELECT s.*
                FROM ofs_technician_operational_state s
                JOIN ofs_resource_hierarchy h ON h.resource_id=s.resource_id
                WHERE s.work_date=%s AND h.root_resource_id=%s
                  AND h.resource_type IN ('TCV','TCP','TCW')
                ORDER BY s.resource_id
                """,
                (work_date, root_resource_id),
            )
            states = [dict(row) for row in (cur.fetchall() or [])]

            cur.execute(
                """
                SELECT a.activity_id,a.work_date,a.resource_id,a.status,a.appt_number,a.activity_type,
                       a.record_type,a.start_time,a.duration_minutes,a.time_slot,a.is_black,a.customer_name,
                       a.customer_state,
                       a.resource_timezone_iana
                FROM ofs_activity_operational_state a
                JOIN ofs_resource_hierarchy h ON h.resource_id=a.resource_id
                WHERE a.work_date=%s AND h.root_resource_id=%s
                  AND h.resource_type IN ('TCV','TCP','TCW')
                ORDER BY a.resource_id,a.activity_id
                """,
                (work_date, root_resource_id),
            )
            activities = [dict(row) for row in (cur.fetchall() or [])]

            cur.execute(
                """
                SELECT source_name,last_success_at,status,error_code
                FROM ofs_operational_sync_state
                WHERE source_name IN ('events','activities','calendars','routes','hierarchy')
                """
            )
            health = {
                str(row["source_name"]): dict(row)
                for row in (cur.fetchall() or [])
                if row.get("source_name")
            }
            return SourceSnapshot(hierarchy, states, activities, health)
        finally:
            cur.close()

    @staticmethod
    def _serialize_snapshot(row: Optional[Mapping[str, Any]]) -> Optional[dict]:
        if not row:
            return None
        payload = {}
        raw = row.get("payload_json")
        if raw:
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                payload = {}
        now = utc_now_naive()
        expires_at = row.get("expires_at")
        remaining = max(int((expires_at - now).total_seconds()), 0) if expires_at else 0
        refresh_started_at = row.get("refresh_started_at")
        refresh_in_progress = (
            row.get("status") == "refreshing"
            and refresh_started_at is not None
            and (now - refresh_started_at).total_seconds() <= SNAPSHOT_REFRESH_STALE_SECONDS
        )
        return {
            "scope_key": row.get("scope_key"),
            "work_date": row.get("work_date").isoformat() if row.get("work_date") else None,
            "status": row.get("status") or "missing",
            "payload": payload,
            "has_payload": bool(payload),
            "refreshed_at": _dt_text(row.get("refreshed_at")),
            "expires_at": _dt_text(expires_at),
            "refresh_started_at": _dt_text(refresh_started_at),
            "refresh_finished_at": _dt_text(row.get("refresh_finished_at")),
            "requested_by_username": row.get("requested_by_username"),
            "error_text": row.get("error_text"),
            "refresh_in_progress": refresh_in_progress,
            "refresh_allowed": (not expires_at or expires_at <= now) and not refresh_in_progress,
            "remaining_seconds": remaining,
        }


class OperationalMonitorService:
    def __init__(self, repository: Optional[MySQLOperationalMonitorRepository] = None):
        self.repository = repository or MySQLOperationalMonitorRepository()

    def get_snapshot(self, scope_key: str) -> dict:
        scope = resolve_scope(scope_key)
        snapshot = self.repository.load_snapshot(scope["key"])
        if snapshot:
            snapshot["scope"] = scope
            return snapshot
        return {
            "scope_key": scope["key"],
            "scope": scope,
            "status": "missing",
            "payload": {},
            "has_payload": False,
            "refreshed_at": None,
            "expires_at": None,
            "error_text": None,
            "refresh_in_progress": False,
            "refresh_allowed": True,
            "remaining_seconds": 0,
        }

    def refresh(self, scope_key: str, *, actor_id: Optional[int], actor_username: Optional[str]) -> dict:
        scope = resolve_scope(scope_key)
        now = utc_now_naive()
        work_date = operational_work_date(now.replace(tzinfo=timezone.utc))
        conn = self.repository.connection_factory()
        cur = conn.cursor(dictionary=True)
        lock_acquired = False
        started = now
        try:
            cur.execute("SELECT GET_LOCK(%s,0) AS acquired", (SNAPSHOT_LOCK_NAME,))
            lock_acquired = bool((cur.fetchone() or {}).get("acquired"))
            if not lock_acquired:
                raise MonitorRefreshInProgress("Outro usuário já está atualizando o monitor operacional.")

            cur.execute(
                """
                SELECT scope_key,work_date,status,payload_json,refreshed_at,expires_at,
                       refresh_started_at,refresh_finished_at,requested_by_username,error_text
                FROM ofs_operational_monitor_snapshot
                WHERE scope_key=%s FOR UPDATE
                """,
                (scope["key"],),
            )
            current_row = cur.fetchone()
            if current_row and current_row.get("expires_at") and current_row["expires_at"] > now:
                conn.rollback()
                snapshot = self.repository._serialize_snapshot(current_row) or {}
                snapshot["scope"] = scope
                raise MonitorRefreshCooldown(snapshot)

            cur.execute(
                """
                INSERT INTO ofs_operational_monitor_snapshot
                    (scope_key,work_date,status,refresh_started_at,requested_by_user_id,
                     requested_by_username,error_text,updated_at)
                VALUES (%s,%s,'refreshing',%s,%s,%s,NULL,%s)
                ON DUPLICATE KEY UPDATE work_date=VALUES(work_date),status='refreshing',
                    refresh_started_at=VALUES(refresh_started_at),requested_by_user_id=VALUES(requested_by_user_id),
                    requested_by_username=VALUES(requested_by_username),error_text=NULL,updated_at=VALUES(updated_at)
                """,
                (scope["key"], work_date, started, actor_id, actor_username, now),
            )
            conn.commit()

            source = self.repository.load_source(conn, scope, work_date)
            payload = build_monitor_payload(source, scope, work_date, now=now.replace(tzinfo=timezone.utc))
            finished = utc_now_naive()
            expires_at = finished + timedelta(seconds=SNAPSHOT_TTL_SECONDS)
            payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=_json_default)
            cur.execute(
                """
                UPDATE ofs_operational_monitor_snapshot
                SET work_date=%s,status='ready',payload_json=%s,refreshed_at=%s,expires_at=%s,
                    refresh_finished_at=%s,error_text=NULL,updated_at=%s
                WHERE scope_key=%s
                """,
                (work_date, payload_json, finished, expires_at, finished, finished, scope["key"]),
            )
            cur.execute(
                """
                INSERT INTO ofs_operational_monitor_refresh_log
                    (scope_key,status,requested_by_user_id,requested_by_username,started_at,finished_at,
                     technicians_count,activities_count,error_text)
                VALUES (%s,'ready',%s,%s,%s,%s,%s,%s,NULL)
                """,
                (
                    scope["key"], actor_id, actor_username, started, finished,
                    payload["technicians_count"], payload["diagnostics"]["activities_count"],
                ),
            )
            conn.commit()
            return self.get_snapshot(scope["key"])
        except (MonitorRefreshInProgress, MonitorRefreshCooldown):
            raise
        except Exception as exc:
            conn.rollback()
            error_text = sanitize_operational_error(exc, limit=500)
            failed_at = utc_now_naive()
            try:
                cur.execute(
                    """
                    UPDATE ofs_operational_monitor_snapshot
                    SET status='failed',refresh_finished_at=%s,error_text=%s,updated_at=%s
                    WHERE scope_key=%s
                    """,
                    (failed_at, error_text, failed_at, scope["key"]),
                )
                cur.execute(
                    """
                    INSERT INTO ofs_operational_monitor_refresh_log
                        (scope_key,status,requested_by_user_id,requested_by_username,started_at,finished_at,error_text)
                    VALUES (%s,'failed',%s,%s,%s,%s,%s)
                    """,
                    (scope["key"], actor_id, actor_username, started, failed_at, error_text),
                )
                conn.commit()
            except Exception:
                conn.rollback()
            raise
        finally:
            if lock_acquired:
                try:
                    cur.execute("SELECT RELEASE_LOCK(%s)", (SNAPSHOT_LOCK_NAME,))
                    cur.fetchone()
                except Exception:
                    pass
            cur.close()
            conn.close()
