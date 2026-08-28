from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone, tzinfo
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ofs.config import get_ofs_root_resource_id


SCHEDULE_WORKING = "WORKING"
SCHEDULE_NON_WORKING = "NON_WORKING"
SCHEDULE_ON_CALL = "ON_CALL"
SCHEDULE_UNKNOWN = "DESCONHECIDA"

SCHEDULE_MODE_CALENDAR = "CALENDAR"
SCHEDULE_MODE_FALLBACK = "FALLBACK_HORARIO_NEGOCIO"
SCHEDULE_MODE_NONE = "SEM_HORARIO"

ROUTE_NO_SCHEDULE = "SEM_ESCALA"
ROUTE_WAITING = "AGUARDANDO_ATIVACAO"
ROUTE_ACTIVE = "ATIVA"
ROUTE_ENDED = "ENCERRADA"
ROUTE_UNKNOWN = "DESCONHECIDA"

SEVERITY_NORMAL = "NORMAL"
SEVERITY_ATTENTION = "ATENCAO"
SEVERITY_ALERT = "ALERTA"
SEVERITY_UNKNOWN = "DESCONHECIDA"

INTEGRITY_OK = "OK"
INTEGRITY_STALE = "DADOS_DESATUALIZADOS"
INTEGRITY_UNKNOWN = "DESCONHECIDA"

ALERT_ROUTE_NOT_STARTED = "ROUTE_NOT_STARTED"
ALERT_ROUTE_ACTIVE_AFTER_SHIFT = "ROUTE_ACTIVE_AFTER_SHIFT"
ALERT_ACTIVITY_OPEN_AFTER_SHIFT = "ACTIVITY_OPEN_AFTER_SHIFT"

SOURCE_EVENTS = "events"
SOURCE_ACTIVITIES = "activities"
SOURCE_CALENDARS = "calendars"
SOURCE_ROUTES = "routes"
RUNTIME_SOURCES = (SOURCE_EVENTS, SOURCE_ACTIVITIES, SOURCE_CALENDARS)

_OFFSET_RE = re.compile(r"UTC\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?", re.IGNORECASE)


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    text = _clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _clean(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_hhmm(value: str, default: dt_time) -> dt_time:
    try:
        return datetime.strptime(value.strip(), "%H:%M").time()
    except (AttributeError, TypeError, ValueError):
        return default


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(int(os.getenv(name, default)), minimum)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class AlertRuleSettings:
    fallback_shift_start: dt_time = dt_time(8, 0)
    fallback_shift_end: dt_time = dt_time(19, 0)
    activation_tolerance_minutes: int = 15
    post_shift_tolerance_minutes: int = 15
    events_stale_seconds: int = 3 * 60
    activities_stale_seconds: int = 30 * 60
    calendars_stale_seconds: int = 90 * 60
    timezone_fallback: str = "America/Sao_Paulo"

    @classmethod
    def from_env(cls) -> "AlertRuleSettings":
        fallback_tz = (
            os.getenv("OFS_OPERATIONAL_ALERT_TIMEZONE")
            or os.getenv("DASHBOARD_TIMEZONE")
            or "America/Sao_Paulo"
        ).strip()
        return cls(
            fallback_shift_start=_parse_hhmm(
                os.getenv("OFS_OPERATIONAL_ALERT_FALLBACK_START", "08:00"), dt_time(8, 0)
            ),
            fallback_shift_end=_parse_hhmm(
                os.getenv("OFS_OPERATIONAL_ALERT_FALLBACK_END", "19:00"), dt_time(19, 0)
            ),
            activation_tolerance_minutes=_int_env(
                "OFS_OPERATIONAL_ALERT_ACTIVATION_TOLERANCE_MINUTES", 15
            ),
            post_shift_tolerance_minutes=_int_env(
                "OFS_OPERATIONAL_ALERT_POST_SHIFT_TOLERANCE_MINUTES", 15, minimum=0
            ),
            events_stale_seconds=_int_env("OFS_OPERATIONAL_ALERT_EVENTS_STALE_SECONDS", 180),
            activities_stale_seconds=_int_env(
                "OFS_OPERATIONAL_ALERT_ACTIVITIES_STALE_SECONDS", 1800
            ),
            calendars_stale_seconds=_int_env(
                "OFS_OPERATIONAL_ALERT_CALENDARS_STALE_SECONDS", 5400
            ),
            timezone_fallback=fallback_tz or "America/Sao_Paulo",
        )

    @property
    def stale_thresholds(self) -> Dict[str, int]:
        return {
            SOURCE_EVENTS: self.events_stale_seconds,
            SOURCE_ACTIVITIES: self.activities_stale_seconds,
            SOURCE_CALENDARS: self.calendars_stale_seconds,
        }


def _timezone_from_offset_description(value: str) -> Optional[tzinfo]:
    match = _OFFSET_RE.search(value or "")
    if not match:
        return None
    sign = 1 if match.group(1) == "+" else -1
    hours = int(match.group(2))
    minutes = int(match.group(3) or 0)
    if hours > 23 or minutes > 59:
        return None
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def resolve_technician_timezone(row: Mapping[str, Any], settings: AlertRuleSettings) -> tuple[tzinfo, str, str]:
    iana = _clean(row.get("resource_timezone_iana"))
    if iana:
        try:
            return ZoneInfo(iana), iana, "RESOURCE_IANA"
        except (ZoneInfoNotFoundError, ValueError):
            pass

    descriptive = _clean(row.get("resource_timezone"))
    if descriptive:
        try:
            return ZoneInfo(descriptive), descriptive, "RESOURCE_IANA"
        except (ZoneInfoNotFoundError, ValueError):
            offset_tz = _timezone_from_offset_description(descriptive)
            if offset_tz is not None:
                offset = offset_tz.utcoffset(None) or timedelta(0)
                total_minutes = int(offset.total_seconds() // 60)
                sign = "+" if total_minutes >= 0 else "-"
                total_minutes = abs(total_minutes)
                name = f"UTC{sign}{total_minutes // 60:02d}:{total_minutes % 60:02d}"
                return offset_tz, name, "RESOURCE_OFFSET"

    try:
        return ZoneInfo(settings.timezone_fallback), settings.timezone_fallback, "FALLBACK"
    except (ZoneInfoNotFoundError, ValueError):
        return timezone.utc, "UTC", "FALLBACK_UTC"


def _health_age_seconds(last_success_at: Optional[datetime], now_utc: datetime) -> Optional[float]:
    if last_success_at is None:
        return None
    if last_success_at.tzinfo is None:
        last_success_utc = last_success_at.replace(tzinfo=timezone.utc)
    else:
        last_success_utc = last_success_at.astimezone(timezone.utc)
    return max((now_utc - last_success_utc).total_seconds(), 0.0)


def evaluate_source_health(
    source: str,
    row: Optional[Mapping[str, Any]],
    *,
    now_utc: datetime,
    threshold_seconds: Optional[int],
) -> dict:
    if not row:
        return {
            "source": source,
            "state": INTEGRITY_UNKNOWN,
            "status": None,
            "last_success_at": None,
            "age_seconds": None,
            "threshold_seconds": threshold_seconds,
            "caught_up": None,
        }

    status = (_clean(row.get("status")) or "").lower()
    last_success = _as_datetime(row.get("last_success_at"))
    age = _health_age_seconds(last_success, now_utc)
    caught_up = row.get("caught_up")

    if status in {"error", "failed", "failure"}:
        state = INTEGRITY_STALE
    elif last_success is None:
        state = INTEGRITY_UNKNOWN
    elif threshold_seconds is not None and age is not None and age > threshold_seconds:
        state = INTEGRITY_STALE
    elif caught_up is False:
        state = INTEGRITY_STALE
    else:
        # 'running' remains healthy while the previous successful cycle is still fresh.
        state = INTEGRITY_OK

    return {
        "source": source,
        "state": state,
        "status": status or None,
        "last_success_at": last_success.isoformat() if last_success else None,
        "age_seconds": round(age, 3) if age is not None else None,
        "threshold_seconds": threshold_seconds,
        "caught_up": bool(caught_up) if isinstance(caught_up, bool) else None,
    }


def _normalize_schedule_state(record_type: Any) -> str:
    value = (_clean(record_type) or "").lower().replace("_", "-")
    if value in {"working", "extra-working", "extraworking"}:
        # OFS usa extra_working para jornadas extraordinárias. Para o monitor
        # operacional isso continua sendo uma jornada válida do dia e deve
        # participar de working_count e da taxa de ativação.
        return SCHEDULE_WORKING
    if value in {"non-working", "nonworking"}:
        return SCHEDULE_NON_WORKING
    if value in {"on-call", "oncall"}:
        return SCHEDULE_ON_CALL
    return SCHEDULE_UNKNOWN


def _localize_wall_clock(value: Optional[datetime], tz: tzinfo) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def _effective_shift(
    row: Mapping[str, Any],
    schedule_state: str,
    tz: tzinfo,
    settings: AlertRuleSettings,
) -> tuple[Optional[datetime], Optional[datetime], str, List[str]]:
    if schedule_state != SCHEDULE_WORKING:
        return None, None, SCHEDULE_MODE_NONE, []

    work_date = _as_date(row.get("work_date"))
    if work_date is None:
        return None, None, SCHEDULE_MODE_NONE, ["WORK_DATE_INVALID"]

    start = _localize_wall_clock(_as_datetime(row.get("calendar_start_at")), tz)
    end = _localize_wall_clock(_as_datetime(row.get("calendar_end_at")), tz)
    reasons: List[str] = []
    mode = SCHEDULE_MODE_CALENDAR

    if start is None:
        start = datetime.combine(work_date, settings.fallback_shift_start, tzinfo=tz)
        reasons.append("SHIFT_START_FALLBACK")
        mode = SCHEDULE_MODE_FALLBACK
    if end is None:
        end = datetime.combine(work_date, settings.fallback_shift_end, tzinfo=tz)
        reasons.append("SHIFT_END_FALLBACK")
        mode = SCHEDULE_MODE_FALLBACK

    if end <= start:
        end += timedelta(days=1)
        reasons.append("SHIFT_CROSSES_MIDNIGHT")
    return start, end, mode, reasons


def _route_state(row: Mapping[str, Any], schedule_state: str, tz: tzinfo) -> tuple[str, List[str]]:
    started = _localize_wall_clock(_as_datetime(row.get("route_started_at")), tz)
    reactivated = _localize_wall_clock(_as_datetime(row.get("route_reactivated_at")), tz)
    ended = _localize_wall_clock(_as_datetime(row.get("route_ended_at")), tz)
    starts = [value for value in (started, reactivated) if value is not None]
    latest_start = max(starts) if starts else None
    reasons: List[str] = []

    if latest_start is not None:
        if ended is None or ended < latest_start:
            return ROUTE_ACTIVE, reasons
        return ROUTE_ENDED, reasons

    if ended is not None:
        reasons.append("ROUTE_END_WITHOUT_START")
        return ROUTE_UNKNOWN, reasons

    raw_state = (_clean(row.get("route_state")) or "").lower()
    if raw_state in {"active", "started"}:
        # A compact legacy row can know the state even if exact timestamps were unavailable.
        return ROUTE_ACTIVE, ["ROUTE_STATE_WITHOUT_TIMESTAMP"]
    if raw_state in {"ended", "closed", "deactivated"}:
        return ROUTE_ENDED, ["ROUTE_STATE_WITHOUT_TIMESTAMP"]

    if schedule_state == SCHEDULE_NON_WORKING:
        return ROUTE_NO_SCHEDULE, reasons
    if schedule_state == SCHEDULE_WORKING:
        return ROUTE_WAITING, reasons
    if schedule_state == SCHEDULE_ON_CALL:
        # Plantão is preserved but does not imply activation obligation.
        return ROUTE_NO_SCHEDULE, reasons
    return ROUTE_UNKNOWN, reasons


def _combine_integrity(source_states: Iterable[str]) -> str:
    states = set(source_states)
    if INTEGRITY_STALE in states:
        return INTEGRITY_STALE
    if INTEGRITY_UNKNOWN in states:
        return INTEGRITY_UNKNOWN
    return INTEGRITY_OK


class TechnicianAlertClassifier:
    """Pure classifier over the MySQL read model created by Demanda 06.

    No Oracle/OFS calls are made here. `classify_batch` accepts rows already loaded in bulk
    so the same classifier can be reused by the local Flask API in Demanda 08.
    """

    def __init__(self, settings: Optional[AlertRuleSettings] = None):
        self.settings = settings or AlertRuleSettings.from_env()

    def classify_one(
        self,
        row: Mapping[str, Any],
        health_by_source: Mapping[str, Mapping[str, Any]],
        *,
        now: Optional[datetime] = None,
    ) -> dict:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now_utc = now.astimezone(timezone.utc)

        tz, tz_name, tz_source = resolve_technician_timezone(row, self.settings)
        local_now = now.astimezone(tz)
        work_date = _as_date(row.get("work_date"))
        schedule_state = _normalize_schedule_state(row.get("calendar_record_type"))
        shift_start, shift_end, schedule_mode, schedule_reasons = _effective_shift(
            row, schedule_state, tz, self.settings
        )
        route_state, route_reasons = _route_state(row, schedule_state, tz)

        freshness: Dict[str, dict] = {}
        for source in (SOURCE_EVENTS, SOURCE_ACTIVITIES, SOURCE_CALENDARS):
            freshness[source] = evaluate_source_health(
                source,
                health_by_source.get(source),
                now_utc=now_utc,
                threshold_seconds=self.settings.stale_thresholds[source],
            )
        # Routes is deliberately informational: it is baseline/recovery health, not minute freshness.
        freshness[SOURCE_ROUTES] = evaluate_source_health(
            SOURCE_ROUTES,
            health_by_source.get(SOURCE_ROUTES),
            now_utc=now_utc,
            threshold_seconds=None,
        )

        required_integrity_states = [freshness[SOURCE_EVENTS]["state"]]
        if schedule_state in {SCHEDULE_WORKING, SCHEDULE_NON_WORKING, SCHEDULE_ON_CALL, SCHEDULE_UNKNOWN}:
            required_integrity_states.append(freshness[SOURCE_CALENDARS]["state"])
        required_integrity_states.append(freshness[SOURCE_ACTIVITIES]["state"])
        integrity_state = _combine_integrity(required_integrity_states)

        integrity_codes: List[str] = []
        for source in RUNTIME_SOURCES:
            state = freshness[source]["state"]
            if state == INTEGRITY_STALE:
                integrity_codes.append(f"{source.upper()}_STALE")
            elif state == INTEGRITY_UNKNOWN:
                integrity_codes.append(f"{source.upper()}_UNKNOWN")

        if schedule_state == SCHEDULE_UNKNOWN and freshness[SOURCE_CALENDARS]["state"] == INTEGRITY_OK:
            if integrity_state == INTEGRITY_OK:
                integrity_state = INTEGRITY_UNKNOWN
            integrity_codes.append("CALENDAR_RECORD_UNKNOWN")
        if "ROUTE_END_WITHOUT_START" in route_reasons:
            if integrity_state == INTEGRITY_OK:
                integrity_state = INTEGRITY_UNKNOWN
            integrity_codes.append("ROUTE_FACTS_INCONSISTENT")
        if route_state == ROUTE_WAITING and freshness[SOURCE_ROUTES]["state"] != INTEGRITY_OK:
            # Route age is intentionally ignored, but a missing/failed baseline cannot support
            # an absence-based "not started" decision.
            route_integrity = freshness[SOURCE_ROUTES]["state"]
            if route_integrity == INTEGRITY_STALE:
                integrity_state = INTEGRITY_STALE
                integrity_codes.append("ROUTES_BASELINE_STALE")
            elif integrity_state == INTEGRITY_OK:
                integrity_state = INTEGRITY_UNKNOWN
                integrity_codes.append("ROUTES_BASELINE_UNKNOWN")

        decision_reasons = list(schedule_reasons) + list(route_reasons)
        if tz_source.startswith("FALLBACK"):
            decision_reasons.append("TIMEZONE_FALLBACK")
        elif tz_source == "RESOURCE_OFFSET":
            decision_reasons.append("TIMEZONE_OFFSET_FALLBACK")

        if work_date is None:
            integrity_state = INTEGRITY_UNKNOWN
            integrity_codes.append("WORK_DATE_UNKNOWN")
        elif work_date != local_now.date():
            # Historical/future retained rows remain classifiable as facts, but time-driven alerts
            # must not be emitted against a different local operational day.
            decision_reasons.append("OUTSIDE_LOCAL_OPERATIONAL_DAY")
            if integrity_state == INTEGRITY_OK:
                integrity_state = INTEGRITY_UNKNOWN
            integrity_codes.append("OUTSIDE_LOCAL_OPERATIONAL_DAY")

        started_count = int(row.get("started_count") or 0)
        suspended_count = int(row.get("suspended_count") or 0)
        alert_codes: List[str] = []
        severity = SEVERITY_NORMAL

        same_operational_day = work_date is not None and work_date == local_now.date()
        events_usable_for_absence_rules = freshness[SOURCE_EVENTS]["state"] == INTEGRITY_OK
        activities_usable_for_absence_rules = freshness[SOURCE_ACTIVITIES]["state"] == INTEGRITY_OK
        calendar_usable_for_absence_rules = freshness[SOURCE_CALENDARS]["state"] == INTEGRITY_OK
        routes_baseline_usable_for_absence_rules = freshness[SOURCE_ROUTES]["state"] == INTEGRITY_OK

        post_shift_at = (
            shift_end + timedelta(minutes=self.settings.post_shift_tolerance_minutes)
            if shift_end is not None
            else None
        )
        activation_due_at = (
            shift_start + timedelta(minutes=self.settings.activation_tolerance_minutes)
            if shift_start is not None
            else None
        )

        # Positive evidence is preserved even when freshness is degraded. This intentionally
        # keeps operational anomaly and integrity as independent dimensions.
        if (
            same_operational_day
            and schedule_state == SCHEDULE_WORKING
            and post_shift_at is not None
            and local_now >= post_shift_at
            and (started_count > 0 or suspended_count > 0)
        ):
            alert_codes.append(ALERT_ACTIVITY_OPEN_AFTER_SHIFT)
            severity = SEVERITY_ALERT
        elif (
            same_operational_day
            and schedule_state == SCHEDULE_WORKING
            and post_shift_at is not None
            and local_now >= post_shift_at
            and route_state == ROUTE_ACTIVE
            and started_count == 0
            and suspended_count == 0
            and events_usable_for_absence_rules
            and activities_usable_for_absence_rules
        ):
            alert_codes.append(ALERT_ROUTE_ACTIVE_AFTER_SHIFT)
            severity = SEVERITY_ATTENTION
        elif (
            same_operational_day
            and schedule_state == SCHEDULE_WORKING
            and activation_due_at is not None
            and local_now >= activation_due_at
            and route_state == ROUTE_WAITING
            and events_usable_for_absence_rules
            and calendar_usable_for_absence_rules
            and routes_baseline_usable_for_absence_rules
        ):
            alert_codes.append(ALERT_ROUTE_NOT_STARTED)
            severity = SEVERITY_ATTENTION

        # Stale/unknown global health must not be presented as a reliable NORMAL when no
        # positive anomaly is already known.
        if not alert_codes and integrity_state != INTEGRITY_OK:
            severity = SEVERITY_UNKNOWN

        return {
            "resource_id": _clean(row.get("resource_id")),
            "work_date": work_date.isoformat() if work_date else None,
            "schedule_state": schedule_state,
            "schedule_mode": schedule_mode,
            "route_state": route_state,
            "operational_severity": severity,
            "alert_codes": alert_codes,
            "integrity_state": integrity_state,
            "integrity_codes": sorted(set(integrity_codes)),
            "effective_shift_start": shift_start.isoformat() if shift_start else None,
            "effective_shift_end": shift_end.isoformat() if shift_end else None,
            "activation_due_at": activation_due_at.isoformat() if activation_due_at else None,
            "post_shift_due_at": post_shift_at.isoformat() if post_shift_at else None,
            "local_now": local_now.isoformat(),
            "timezone": tz_name,
            "timezone_source": tz_source,
            "route_started_at": _as_datetime(row.get("route_started_at")).isoformat() if _as_datetime(row.get("route_started_at")) else None,
            "route_reactivated_at": _as_datetime(row.get("route_reactivated_at")).isoformat() if _as_datetime(row.get("route_reactivated_at")) else None,
            "route_ended_at": _as_datetime(row.get("route_ended_at")).isoformat() if _as_datetime(row.get("route_ended_at")) else None,
            "started_count": started_count,
            "suspended_count": suspended_count,
            "open_activity_count": int(row.get("open_activity_count") or 0),
            "freshness": freshness,
            "decision_reasons": sorted(set(decision_reasons)),
        }

    def classify_batch(
        self,
        rows: Sequence[Mapping[str, Any]],
        health_by_source: Mapping[str, Mapping[str, Any]],
        *,
        now: Optional[datetime] = None,
    ) -> List[dict]:
        now = now or datetime.now(timezone.utc)
        return [self.classify_one(row, health_by_source, now=now) for row in rows]


class MySQLTechnicianAlertReadRepository:
    """Bulk-only local reader for Demanda 07/08. Never calls Oracle/OFS."""

    def __init__(self, connection_factory: Optional[Callable] = None):
        if connection_factory is None:
            from database.connection import get_connection

            connection_factory = get_connection
        self.connection_factory = connection_factory

    def load_snapshot(self, work_date: date, root_resource_id: Optional[str] = None) -> tuple[List[dict], Dict[str, dict]]:
        root_resource_id = root_resource_id or get_ofs_root_resource_id()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT s.*, h.resource_name, h.resource_type
                FROM ofs_technician_operational_state s
                JOIN ofs_resource_hierarchy h ON h.resource_id = s.resource_id
                WHERE s.work_date = %s
                  AND h.root_resource_id = %s
                  AND h.resource_type IN ('TCV','TCP','TCW')
                ORDER BY s.resource_id
                """,
                (work_date, root_resource_id),
            )
            rows = list(cur.fetchall() or [])

            cur.execute(
                """
                SELECT source_name,last_started_at,last_success_at,last_finished_at,status,error_code,error_message,updated_at
                FROM ofs_operational_sync_state
                WHERE source_name IN ('events','activities','calendars','routes')
                """
            )
            health = {
                str(item["source_name"]): dict(item)
                for item in (cur.fetchall() or [])
                if item.get("source_name")
            }
            return rows, health
        finally:
            cur.close()
            conn.close()


class TechnicianAlertService:
    def __init__(
        self,
        repository: Optional[MySQLTechnicianAlertReadRepository] = None,
        classifier: Optional[TechnicianAlertClassifier] = None,
    ):
        self.repository = repository or MySQLTechnicianAlertReadRepository()
        self.classifier = classifier or TechnicianAlertClassifier()

    def classify_date(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> List[dict]:
        rows, health = self.repository.load_snapshot(work_date, root_resource_id=root_resource_id)
        return self.classifier.classify_batch(rows, health, now=now)
