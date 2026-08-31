from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from ofs.config import get_ofs_root_resource_id
from services.ofs_technician_alert_service import (
    ALERT_ACTIVITY_OPEN_AFTER_SHIFT,
    ALERT_ROUTE_ACTIVE_AFTER_SHIFT,
    ALERT_ROUTE_NOT_STARTED,
    INTEGRITY_OK,
    INTEGRITY_STALE,
    INTEGRITY_UNKNOWN,
    ROUTE_ACTIVE,
    ROUTE_ENDED,
    ROUTE_NO_SCHEDULE,
    ROUTE_UNKNOWN,
    ROUTE_WAITING,
    SCHEDULE_NON_WORKING,
    SCHEDULE_ON_CALL,
    SCHEDULE_UNKNOWN,
    SCHEDULE_WORKING,
    SEVERITY_ALERT,
    SEVERITY_ATTENTION,
    SEVERITY_NORMAL,
    SEVERITY_UNKNOWN,
    SOURCE_ACTIVITIES,
    SOURCE_CALENDARS,
    SOURCE_EVENTS,
    SOURCE_ROUTES,
    AlertRuleSettings,
    TechnicianAlertClassifier,
    evaluate_source_health,
)


TECHNICIAN_RESOURCE_TYPES = frozenset({"TCV", "TCP", "TCW"})
HEALTH_SOURCES = (SOURCE_EVENTS, SOURCE_ACTIVITIES, SOURCE_CALENDARS, SOURCE_ROUTES)
RUNTIME_HEALTH_SOURCES = (SOURCE_EVENTS, SOURCE_ACTIVITIES, SOURCE_CALENDARS)
HIERARCHY_HEALTH_SOURCE = "hierarchy"
DEFAULT_HIERARCHY_STALE_SECONDS = 2 * 60 * 60
ROUTE_HISTORY_RETENTION_DAYS = 7
ROUTE_HISTORY_DISPLAY_TIMEZONE = "America/Sao_Paulo"

SCHEDULE_KEYS = {
    SCHEDULE_WORKING: "working",
    SCHEDULE_NON_WORKING: "non_working",
    SCHEDULE_ON_CALL: "on_call",
    SCHEDULE_UNKNOWN: "unknown",
}
ROUTE_KEYS = {
    ROUTE_NO_SCHEDULE: "sem_escala",
    ROUTE_WAITING: "aguardando_ativacao",
    ROUTE_ACTIVE: "ativa",
    ROUTE_ENDED: "encerrada",
    ROUTE_UNKNOWN: "desconhecida",
}
SEVERITY_KEYS = {
    SEVERITY_NORMAL: "normal",
    SEVERITY_ATTENTION: "atencao",
    SEVERITY_ALERT: "alerta",
    SEVERITY_UNKNOWN: "desconhecida",
}
INTEGRITY_KEYS = {
    INTEGRITY_OK: "ok",
    INTEGRITY_STALE: "dados_desatualizados",
    INTEGRITY_UNKNOWN: "desconhecida",
}
KNOWN_ALERT_CODES = (
    ALERT_ROUTE_NOT_STARTED,
    ALERT_ROUTE_ACTIVE_AFTER_SHIFT,
    ALERT_ACTIVITY_OPEN_AFTER_SHIFT,
)
TREE_FILTERS = frozenset({
    "problems",
    "alert",
    "attention",
    "waiting_route",
    "active_route",
    "started",
    "suspended",
    "open",
})

HIERARCHY_SQL = """
    SELECT
        resource_id,
        parent_resource_id,
        resource_name,
        resource_type,
        status,
        timezone,
        depth,
        root_resource_id,
        last_seen_at,
        updated_at
    FROM ofs_resource_hierarchy
    WHERE root_resource_id = %s
    ORDER BY depth, resource_id
"""

OPERATIONAL_STATE_SQL = """
    SELECT s.*
    FROM ofs_technician_operational_state s
    JOIN ofs_resource_hierarchy h ON h.resource_id = s.resource_id
    WHERE s.work_date = %s
      AND h.root_resource_id = %s
      AND h.resource_type IN ('TCV','TCP','TCW')
    ORDER BY s.resource_id
"""

HEALTH_SQL = """
    SELECT
        source_name,
        last_started_at,
        last_success_at,
        last_finished_at,
        status,
        error_code,
        error_message,
        updated_at
    FROM ofs_operational_sync_state
    WHERE source_name IN ('events','activities','calendars','routes','hierarchy')
"""

ROUTE_HISTORY_SQL = """
    SELECT
        s.work_date,
        s.resource_id,
        s.route_started_at,
        s.route_reactivated_at,
        s.route_ended_at,
        s.resource_timezone,
        s.resource_timezone_iana
    FROM ofs_technician_operational_state s
    JOIN ofs_resource_hierarchy h ON h.resource_id = s.resource_id
    WHERE s.resource_id = %s
      AND s.work_date BETWEEN %s AND %s
      AND h.root_resource_id = %s
      AND h.resource_type IN ('TCV','TCP','TCW')
    ORDER BY s.work_date DESC
"""


@dataclass(frozen=True)
class MonitorSnapshot:
    hierarchy: List[dict]
    operational_rows: List[dict]
    health_by_source: Dict[str, dict]
    query_metrics_ms: Dict[str, float]


class MySQLTechnicianMonitorRepository:
    """Bulk-only reader for the local monitor API. Never calls Oracle/OFS."""

    def __init__(self, connection_factory: Optional[Callable] = None):
        if connection_factory is None:
            from database.connection import get_connection

            connection_factory = get_connection
        self.connection_factory = connection_factory

    def load_snapshot(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str] = None,
    ) -> MonitorSnapshot:
        root_resource_id = root_resource_id or get_ofs_root_resource_id()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        metrics: Dict[str, float] = {}
        try:
            started = time.perf_counter()
            cur.execute(HIERARCHY_SQL, (root_resource_id,))
            hierarchy = [dict(row) for row in (cur.fetchall() or [])]
            metrics["hierarchy_query_ms"] = (time.perf_counter() - started) * 1000.0

            started = time.perf_counter()
            cur.execute(OPERATIONAL_STATE_SQL, (work_date, root_resource_id))
            operational_rows = [dict(row) for row in (cur.fetchall() or [])]
            metrics["operational_query_ms"] = (time.perf_counter() - started) * 1000.0

            started = time.perf_counter()
            cur.execute(HEALTH_SQL)
            health_by_source = {
                str(row["source_name"]): dict(row)
                for row in (cur.fetchall() or [])
                if row.get("source_name")
            }
            metrics["health_query_ms"] = (time.perf_counter() - started) * 1000.0

            return MonitorSnapshot(
                hierarchy=hierarchy,
                operational_rows=operational_rows,
                health_by_source=health_by_source,
                query_metrics_ms=metrics,
            )
        finally:
            cur.close()
            conn.close()

    def load_route_history(
        self,
        resource_id: str,
        *,
        end_date: date,
        retention_days: int = ROUTE_HISTORY_RETENTION_DAYS,
        root_resource_id: Optional[str] = None,
    ) -> Tuple[List[dict], float]:
        root_resource_id = root_resource_id or get_ofs_root_resource_id()
        retention_days = max(1, min(int(retention_days), ROUTE_HISTORY_RETENTION_DAYS))
        start_date = end_date - timedelta(days=retention_days - 1)
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            started = time.perf_counter()
            cur.execute(
                ROUTE_HISTORY_SQL,
                (str(resource_id), start_date, end_date, root_resource_id),
            )
            rows = [dict(row) for row in (cur.fetchall() or [])]
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return rows, elapsed_ms
        finally:
            cur.close()
            conn.close()

    def explain_queries(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str] = None,
    ) -> Dict[str, List[dict]]:
        root_resource_id = root_resource_id or get_ofs_root_resource_id()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            explains: Dict[str, List[dict]] = {}
            for name, sql, params in (
                ("hierarchy", HIERARCHY_SQL, (root_resource_id,)),
                ("operational", OPERATIONAL_STATE_SQL, (work_date, root_resource_id)),
                ("health", HEALTH_SQL, None),
            ):
                cur.execute("EXPLAIN " + sql, params)
                explains[name] = [dict(row) for row in (cur.fetchall() or [])]
            return explains
        finally:
            cur.close()
            conn.close()


class TechnicianMonitorService:
    """Builds compact HTTP-facing contracts from MySQL + Demanda 07 classifier."""

    def __init__(
        self,
        repository: Optional[MySQLTechnicianMonitorRepository] = None,
        classifier: Optional[TechnicianAlertClassifier] = None,
        hierarchy_stale_seconds: int = DEFAULT_HIERARCHY_STALE_SECONDS,
    ):
        self.repository = repository or MySQLTechnicianMonitorRepository()
        self.classifier = classifier or TechnicianAlertClassifier()
        self.settings: AlertRuleSettings = self.classifier.settings
        self.hierarchy_stale_seconds = max(int(hierarchy_stale_seconds), 60)

    def build_summary(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> Tuple[dict, dict]:
        context, metrics = self._build_context(
            work_date,
            root_resource_id=root_resource_id,
            now=now,
        )
        started = time.perf_counter()
        payload = self._summary_payload(context)
        metrics["aggregation_ms"] = (time.perf_counter() - started) * 1000.0
        metrics.update(_serialized_metrics(payload))
        return payload, metrics

    def build_tree(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str] = None,
        now: Optional[datetime] = None,
        mode: str = "children",
        parent_id: Optional[str] = None,
        only_problems: bool = False,
        filter_name: Optional[str] = None,
        detail: bool = False,
    ) -> Tuple[dict, dict]:
        if mode not in {"full", "children"}:
            raise ValueError("mode deve ser 'full' ou 'children'.")
        effective_filter = _normalize_tree_filter(filter_name, only_problems=only_problems)

        context, metrics = self._build_context(
            work_date,
            root_resource_id=root_resource_id,
            now=now,
        )

        started = time.perf_counter()
        tree_payload = self._tree_payload(
            context,
            mode=mode,
            parent_id=parent_id,
            only_problems=only_problems,
            filter_name=effective_filter,
            detail=detail,
        )
        metrics["aggregation_ms"] = (time.perf_counter() - started) * 1000.0
        metrics.update(_serialized_metrics(tree_payload))
        return tree_payload, metrics

    def build_route_history(
        self,
        resource_id: str,
        *,
        end_date: Optional[date] = None,
        root_resource_id: Optional[str] = None,
    ) -> Tuple[dict, dict]:
        resource_id = str(resource_id or "").strip()
        if not resource_id:
            raise ValueError("resource_id é obrigatório.")
        end_date = end_date or default_monitor_work_date(settings=self.settings)
        rows, query_ms = self.repository.load_route_history(
            resource_id,
            end_date=end_date,
            retention_days=ROUTE_HISTORY_RETENTION_DAYS,
            root_resource_id=root_resource_id or get_ofs_root_resource_id(),
        )
        days = [_route_history_day_payload(row) for row in rows]
        days.sort(key=lambda item: item["work_date"], reverse=True)
        payload = {
            "resource_id": resource_id,
            "window_start": (end_date - timedelta(days=ROUTE_HISTORY_RETENTION_DAYS - 1)).isoformat(),
            "window_end": end_date.isoformat(),
            "retention_days": ROUTE_HISTORY_RETENTION_DAYS,
            "data_available": bool(days),
            "display_timezone": _route_history_display_timezone(rows),
            "days": days,
        }
        metrics = {
            "mysql_queries_total": 1,
            "route_history_query_ms": query_ms,
        }
        metrics.update(_serialized_metrics(payload))
        return payload, metrics

    def _build_context(
        self,
        work_date: date,
        *,
        root_resource_id: Optional[str],
        now: Optional[datetime],
    ) -> Tuple[dict, dict]:
        root_resource_id = root_resource_id or get_ofs_root_resource_id()
        total_started = time.perf_counter()
        snapshot = self.repository.load_snapshot(work_date, root_resource_id=root_resource_id)

        hierarchy_by_id = {
            str(row.get("resource_id")): dict(row)
            for row in snapshot.hierarchy
            if row.get("resource_id") is not None
        }
        technician_rows = [
            row
            for row in snapshot.hierarchy
            if str(row.get("resource_type") or "").upper() in TECHNICIAN_RESOURCE_TYPES
        ]
        state_by_id = {
            str(row.get("resource_id")): dict(row)
            for row in snapshot.operational_rows
            if row.get("resource_id") is not None
        }

        classifier_rows: List[dict] = []
        missing_operational_count = 0
        for hierarchy_row in technician_rows:
            resource_id = str(hierarchy_row.get("resource_id"))
            state = state_by_id.get(resource_id)
            if state is None:
                missing_operational_count += 1
                state = {
                    "work_date": work_date,
                    "resource_id": resource_id,
                    "calendar_record_type": None,
                    "resource_timezone": hierarchy_row.get("timezone"),
                    "resource_timezone_iana": None,
                    "started_count": 0,
                    "suspended_count": 0,
                    "open_activity_count": 0,
                }
            classifier_rows.append(state)

        classified_started = time.perf_counter()
        classifications = self.classifier.classify_batch(
            classifier_rows,
            snapshot.health_by_source,
            now=now,
        )
        classification_ms = (time.perf_counter() - classified_started) * 1000.0
        classification_by_id = {
            str(item.get("resource_id")): item
            for item in classifications
            if item.get("resource_id") is not None
        }

        effective_now = now or datetime.now(timezone.utc)
        if effective_now.tzinfo is None:
            effective_now = effective_now.replace(tzinfo=timezone.utc)
        health = self._health_payload(snapshot.health_by_source, effective_now)
        hierarchy_sync = self._hierarchy_sync_payload(snapshot.health_by_source, effective_now)

        metrics = dict(snapshot.query_metrics_ms)
        metrics["mysql_queries_total"] = 3
        metrics["classification_ms"] = classification_ms
        metrics["context_total_ms"] = (time.perf_counter() - total_started) * 1000.0

        return {
            "work_date": work_date,
            "root_resource_id": root_resource_id,
            "hierarchy": snapshot.hierarchy,
            "hierarchy_by_id": hierarchy_by_id,
            "technician_rows": technician_rows,
            "operational_rows": snapshot.operational_rows,
            "classifications": classifications,
            "classification_by_id": classification_by_id,
            "health": health,
            "hierarchy_sync": hierarchy_sync,
            "data_available": bool(snapshot.operational_rows),
            "missing_operational_count": missing_operational_count,
            "operational_resource_ids": frozenset(state_by_id),
            "generated_at": effective_now.astimezone(timezone.utc).isoformat(),
        }, metrics

    def _health_payload(self, health_by_source: Mapping[str, Mapping[str, Any]], now: datetime) -> dict:
        now_utc = now.astimezone(timezone.utc)
        sources: Dict[str, dict] = {}
        for source in HEALTH_SOURCES:
            threshold = self.settings.stale_thresholds.get(source)
            sources[source] = evaluate_source_health(
                source,
                health_by_source.get(source),
                now_utc=now_utc,
                threshold_seconds=threshold,
            )

        runtime_states = {sources[source]["state"] for source in RUNTIME_HEALTH_SOURCES}
        if INTEGRITY_STALE in runtime_states:
            overall = INTEGRITY_STALE
        elif INTEGRITY_UNKNOWN in runtime_states:
            overall = INTEGRITY_UNKNOWN
        else:
            overall = INTEGRITY_OK

        return {
            "overall_integrity": INTEGRITY_KEYS[overall],
            "sources": sources,
            "events_caught_up": sources[SOURCE_EVENTS].get("caught_up"),
        }

    def _hierarchy_sync_payload(
        self,
        health_by_source: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> dict:
        row = health_by_source.get(HIERARCHY_HEALTH_SOURCE)
        evaluated = evaluate_source_health(
            HIERARCHY_HEALTH_SOURCE,
            row,
            now_utc=now.astimezone(timezone.utc),
            threshold_seconds=self.hierarchy_stale_seconds,
        )
        raw_status = str((row or {}).get("status") or "").strip().lower() or None
        if raw_status == "running":
            display_state = "running"
        elif raw_status in {"error", "failed", "failure"}:
            display_state = "error"
        elif evaluated.get("state") == INTEGRITY_STALE:
            display_state = "stale"
        elif evaluated.get("state") == INTEGRITY_UNKNOWN:
            display_state = "unknown"
        else:
            display_state = "ok"

        def iso(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc).isoformat()
            text = str(value).strip()
            return text or None

        last_success_at = evaluated.get("last_success_at")
        return {
            "source": HIERARCHY_HEALTH_SOURCE,
            "status": raw_status,
            "state": display_state,
            "freshness_state": evaluated.get("state"),
            "age_seconds": evaluated.get("age_seconds"),
            "threshold_seconds": self.hierarchy_stale_seconds,
            "last_started_at": iso((row or {}).get("last_started_at")),
            "last_success_at": last_success_at,
            "last_finished_at": iso((row or {}).get("last_finished_at")),
            "error_code": (row or {}).get("error_code"),
            "error_message": (row or {}).get("error_message"),
            # O timestamp de último snapshot válido é também a versão estrutural
            # observável pelo browser. Não depende do health operacional.
            "version": last_success_at,
        }

    def _summary_payload(self, context: Mapping[str, Any]) -> dict:
        schedule = {value: 0 for value in SCHEDULE_KEYS.values()}
        routes = {value: 0 for value in ROUTE_KEYS.values()}
        operational = {value: 0 for value in SEVERITY_KEYS.values()}
        integrity = {value: 0 for value in INTEGRITY_KEYS.values()}
        alert_counts = {code: 0 for code in KNOWN_ALERT_CODES}
        started_count = 0
        suspended_count = 0
        open_activity_count = 0

        for item in context["classifications"]:
            schedule[SCHEDULE_KEYS.get(item.get("schedule_state"), "unknown")] += 1
            routes[ROUTE_KEYS.get(item.get("route_state"), "desconhecida")] += 1
            operational[SEVERITY_KEYS.get(item.get("operational_severity"), "desconhecida")] += 1
            integrity[INTEGRITY_KEYS.get(item.get("integrity_state"), "desconhecida")] += 1
            for code in item.get("alert_codes") or []:
                alert_counts[code] = alert_counts.get(code, 0) + 1
            started_count += int(item.get("started_count") or 0)
            suspended_count += int(item.get("suspended_count") or 0)
            open_activity_count += int(item.get("open_activity_count") or 0)

        return {
            "work_date": context["work_date"].isoformat(),
            "root_resource_id": context["root_resource_id"],
            "data_available": context["data_available"],
            "total_technicians": len(context["technician_rows"]),
            "technicians_with_operational_state": len(context["operational_rows"]),
            "technicians_without_operational_state": context["missing_operational_count"],
            "schedule": schedule,
            "routes": routes,
            "operational": operational,
            "alert_codes": alert_counts,
            "integrity": integrity,
            "started_count": started_count,
            "suspended_count": suspended_count,
            "open_activity_count": open_activity_count,
            "health": context["health"],
            "hierarchy_sync": context["hierarchy_sync"],
            "generated_at": context["generated_at"],
        }

    def _tree_payload(
        self,
        context: Mapping[str, Any],
        *,
        mode: str,
        parent_id: Optional[str],
        only_problems: bool,
        filter_name: Optional[str],
        detail: bool,
    ) -> dict:
        nodes_by_id: Dict[str, dict] = {}
        children_by_parent: Dict[Optional[str], List[str]] = {}
        warnings: List[dict] = []

        for row in context["hierarchy"]:
            resource_id = str(row.get("resource_id"))
            parent_raw = row.get("parent_resource_id")
            parent_id_value = str(parent_raw) if parent_raw is not None else None
            classification = context["classification_by_id"].get(resource_id)
            node = {
                "resource_id": resource_id,
                "parent_resource_id": parent_id_value,
                "resource_name": row.get("resource_name") or resource_id,
                "resource_type": row.get("resource_type"),
                "depth": int(row.get("depth") or 0),
                "has_children": False,
                "aggregates": _empty_aggregates(),
                "technician": _technician_payload(
                    classification,
                    detail=detail,
                    has_operational_state=resource_id in context["operational_resource_ids"],
                ) if classification else None,
            }
            if classification:
                node["aggregates"] = _technician_aggregates(
                    classification,
                    has_operational_state=resource_id in context["operational_resource_ids"],
                )
            nodes_by_id[resource_id] = node
            children_by_parent.setdefault(parent_id_value, []).append(resource_id)

        for node in nodes_by_id.values():
            parent_id_value = node["parent_resource_id"]
            if parent_id_value is not None and parent_id_value not in nodes_by_id:
                warnings.append({
                    "code": "PARENT_NOT_FOUND",
                    "resource_id": node["resource_id"],
                    "parent_resource_id": parent_id_value,
                })

        for parent_key, child_ids in children_by_parent.items():
            if parent_key in nodes_by_id and child_ids:
                nodes_by_id[parent_key]["has_children"] = True

        for node in sorted(nodes_by_id.values(), key=lambda item: item["depth"], reverse=True):
            parent_id_value = node["parent_resource_id"]
            if parent_id_value in nodes_by_id:
                _add_aggregates(nodes_by_id[parent_id_value]["aggregates"], node["aggregates"])

        root_ids = [
            resource_id
            for resource_id, node in nodes_by_id.items()
            if node["parent_resource_id"] is None or node["parent_resource_id"] not in nodes_by_id
        ]
        root_ids.sort(key=lambda rid: (nodes_by_id[rid]["depth"], rid))

        def selected(child_id: str) -> bool:
            return _matches_tree_filter(nodes_by_id[child_id]["aggregates"], filter_name)

        if mode == "children":
            if parent_id is None:
                selected_ids = root_ids
                parent_found = True
            elif parent_id in nodes_by_id:
                selected_ids = sorted(
                    children_by_parent.get(parent_id, []),
                    key=lambda rid: (nodes_by_id[rid]["depth"], rid),
                )
                parent_found = True
            else:
                selected_ids = []
                parent_found = False
            selected_ids = [rid for rid in selected_ids if selected(rid)]
            nodes = [_flat_node(nodes_by_id[rid]) for rid in selected_ids]
        else:
            parent_found = None
            nodes = [
                _nested_node(
                    rid,
                    nodes_by_id,
                    children_by_parent,
                    filter_name=filter_name,
                    path=frozenset(),
                )
                for rid in root_ids
                if selected(rid)
            ]

        clusters: List[dict] = []
        if mode == "children" and parent_id is None:
            for root_id in root_ids:
                for cluster_id in sorted(
                    children_by_parent.get(root_id, []),
                    key=lambda rid: (nodes_by_id[rid]["depth"], rid),
                ):
                    if _matches_tree_filter(nodes_by_id[cluster_id]["aggregates"], filter_name):
                        clusters.append(_cluster_payload(nodes_by_id[cluster_id]))

        return {
            "work_date": context["work_date"].isoformat(),
            "root_resource_id": context["root_resource_id"],
            "data_available": context["data_available"],
            "mode": mode,
            "parent_id": parent_id if mode == "children" else None,
            "parent_found": parent_found,
            "only_problems": only_problems,
            "filter": filter_name,
            "detail": detail,
            "total_nodes": len(nodes_by_id),
            "total_technicians": len(context["technician_rows"]),
            "technicians_with_operational_state": len(context["operational_rows"]),
            "technicians_without_operational_state": context["missing_operational_count"],
            "nodes": nodes,
            "clusters": clusters,
            "health": context["health"],
            "hierarchy_sync": context["hierarchy_sync"],
            "hierarchy_warnings": warnings,
            "generated_at": context["generated_at"],
        }


def _route_history_timezone(row: Mapping[str, Any]) -> ZoneInfo:
    candidates = (
        str(row.get("resource_timezone_iana") or "").strip(),
        ROUTE_HISTORY_DISPLAY_TIMEZONE,
    )
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return ZoneInfo(candidate)
        except (ZoneInfoNotFoundError, ValueError):
            continue
    return ZoneInfo("UTC")


def _route_history_time(value: Any, tz: ZoneInfo) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    # MySQL DATETIME values in this read model are wall-clock values. Keep that
    # semantics when naive; only convert timestamps that already carry timezone.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(tz)
    return parsed.strftime("%H:%M")


def _route_history_day_payload(row: Mapping[str, Any]) -> dict:
    work_date = row.get("work_date")
    if isinstance(work_date, datetime):
        work_date = work_date.date()
    if not isinstance(work_date, date):
        work_date = date.fromisoformat(str(work_date))
    tz = _route_history_timezone(row)
    return {
        "work_date": work_date.isoformat(),
        "date_label": work_date.strftime("%d/%m/%Y"),
        "activation_time": _route_history_time(row.get("route_started_at"), tz),
        "reactivation_time": _route_history_time(row.get("route_reactivated_at"), tz),
        "end_time": _route_history_time(row.get("route_ended_at"), tz),
        "timezone": getattr(tz, "key", str(tz)),
    }


def _route_history_display_timezone(rows: Sequence[Mapping[str, Any]]) -> str:
    if rows:
        return getattr(_route_history_timezone(rows[0]), "key", ROUTE_HISTORY_DISPLAY_TIMEZONE)
    return ROUTE_HISTORY_DISPLAY_TIMEZONE


def default_monitor_work_date(
    *,
    now: Optional[datetime] = None,
    settings: Optional[AlertRuleSettings] = None,
) -> date:
    settings = settings or AlertRuleSettings.from_env()
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    try:
        tz = ZoneInfo(settings.timezone_fallback)
    except (ZoneInfoNotFoundError, ValueError):
        tz = timezone.utc
    return current.astimezone(tz).date()


def _empty_aggregates() -> dict:
    return {
        "technician_count": 0,
        "working_count": 0,
        "waiting_route_count": 0,
        "active_route_count": 0,
        "working_active_route_count": 0,
        "ended_route_count": 0,
        "attention_count": 0,
        "alert_count": 0,
        "integrity_degraded_count": 0,
        "missing_state_count": 0,
        "started_count": 0,
        "suspended_count": 0,
        "open_activity_count": 0,
    }


def _technician_aggregates(classification: Mapping[str, Any], *, has_operational_state: bool = True) -> dict:
    aggregate = _empty_aggregates()
    aggregate["technician_count"] = 1
    aggregate["working_count"] = int(classification.get("schedule_state") == SCHEDULE_WORKING)
    aggregate["waiting_route_count"] = int(classification.get("route_state") == ROUTE_WAITING)
    aggregate["active_route_count"] = int(classification.get("route_state") == ROUTE_ACTIVE)
    aggregate["working_active_route_count"] = int(
        classification.get("schedule_state") == SCHEDULE_WORKING
        and classification.get("route_state") == ROUTE_ACTIVE
    )
    aggregate["ended_route_count"] = int(classification.get("route_state") == ROUTE_ENDED)
    aggregate["attention_count"] = int(classification.get("operational_severity") == SEVERITY_ATTENTION)
    aggregate["alert_count"] = int(classification.get("operational_severity") == SEVERITY_ALERT)
    aggregate["integrity_degraded_count"] = int(classification.get("integrity_state") != INTEGRITY_OK)
    aggregate["missing_state_count"] = int(not has_operational_state)
    aggregate["started_count"] = int(classification.get("started_count") or 0)
    aggregate["suspended_count"] = int(classification.get("suspended_count") or 0)
    aggregate["open_activity_count"] = int(classification.get("open_activity_count") or 0)
    return aggregate


def _add_aggregates(target: dict, source: Mapping[str, Any]) -> None:
    for key in target:
        target[key] += int(source.get(key) or 0)


def _normalize_tree_filter(filter_name: Optional[str], *, only_problems: bool) -> Optional[str]:
    value = str(filter_name or "").strip().lower() or None
    if value is None and only_problems:
        return "problems"
    if value is not None and value not in TREE_FILTERS:
        raise ValueError("filter inválido")
    return value


def _matches_tree_filter(aggregates: Mapping[str, Any], filter_name: Optional[str]) -> bool:
    if filter_name is None:
        return True
    if filter_name == "problems":
        return any(
            int(aggregates.get(key) or 0) > 0
            for key in ("attention_count", "alert_count", "missing_state_count")
        )
    key_by_filter = {
        "alert": "alert_count",
        "attention": "attention_count",
        "waiting_route": "waiting_route_count",
        "active_route": "active_route_count",
        "started": "started_count",
        "suspended": "suspended_count",
        "open": "open_activity_count",
    }
    aggregate_key = key_by_filter.get(filter_name)
    return aggregate_key is not None and int(aggregates.get(aggregate_key) or 0) > 0


def _technician_payload(
    classification: Optional[Mapping[str, Any]],
    *,
    detail: bool,
    has_operational_state: bool,
) -> Optional[dict]:
    if not classification:
        return None
    payload = {
        "has_operational_state": bool(has_operational_state),
        "schedule_state": classification.get("schedule_state"),
        "route_state": classification.get("route_state"),
        "operational_severity": classification.get("operational_severity"),
        "alert_codes": list(classification.get("alert_codes") or []),
        "integrity_state": classification.get("integrity_state"),
        "integrity_codes": list(classification.get("integrity_codes") or []),
        "effective_shift_start": classification.get("effective_shift_start"),
        "effective_shift_end": classification.get("effective_shift_end"),
        "route_started_at": classification.get("route_started_at"),
        "route_reactivated_at": classification.get("route_reactivated_at"),
        "route_ended_at": classification.get("route_ended_at"),
        "started_count": int(classification.get("started_count") or 0),
        "suspended_count": int(classification.get("suspended_count") or 0),
        "open_activity_count": int(classification.get("open_activity_count") or 0),
        "timezone": classification.get("timezone"),
        "local_now": classification.get("local_now"),
    }
    if detail:
        payload["decision_reasons"] = list(classification.get("decision_reasons") or [])
    return payload


def _cluster_payload(node: Mapping[str, Any]) -> dict:
    aggregates = dict(node.get("aggregates") or {})
    working_count = int(aggregates.get("working_count") or 0)
    active_route_count = int(aggregates.get("active_route_count") or 0)
    working_active_route_count = int(aggregates.get("working_active_route_count") or 0)
    activation_percent = None if working_count == 0 else round((working_active_route_count / working_count) * 100.0, 1)
    return {
        "resource_id": node["resource_id"],
        "resource_name": node["resource_name"],
        "resource_type": node["resource_type"],
        "working_count": working_count,
        "active_route_count": active_route_count,
        "working_active_route_count": working_active_route_count,
        "activation_percent": activation_percent,
        "waiting_route_count": int(aggregates.get("waiting_route_count") or 0),
        "alert_count": int(aggregates.get("alert_count") or 0),
    }


def _flat_node(node: Mapping[str, Any]) -> dict:
    result = {
        "resource_id": node["resource_id"],
        "parent_resource_id": node["parent_resource_id"],
        "resource_name": node["resource_name"],
        "resource_type": node["resource_type"],
        "depth": node["depth"],
        "has_children": node["has_children"],
    }
    if node.get("technician"):
        result.update(dict(node["technician"]))
    else:
        result["aggregates"] = dict(node["aggregates"])
    return result


def _nested_node(
    resource_id: str,
    nodes_by_id: Mapping[str, Mapping[str, Any]],
    children_by_parent: Mapping[Optional[str], Sequence[str]],
    *,
    filter_name: Optional[str],
    path: frozenset[str],
) -> dict:
    node = nodes_by_id[resource_id]
    if resource_id in path:
        return {**_flat_node(node), "children": [], "hierarchy_cycle": True}

    next_path = path | {resource_id}
    child_ids = sorted(
        children_by_parent.get(resource_id, []),
        key=lambda rid: (nodes_by_id[rid]["depth"], rid),
    )
    if filter_name is not None:
        child_ids = [rid for rid in child_ids if _matches_tree_filter(nodes_by_id[rid]["aggregates"], filter_name)]

    result = _flat_node(node)
    result["children"] = [
        _nested_node(
            child_id,
            nodes_by_id,
            children_by_parent,
            filter_name=filter_name,
            path=next_path,
        )
        for child_id in child_ids
    ]
    return result


def _serialized_metrics(payload: Mapping[str, Any]) -> dict:
    started = time.perf_counter()
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    return {
        "serialization_ms": (time.perf_counter() - started) * 1000.0,
        "payload_bytes": len(raw),
    }
