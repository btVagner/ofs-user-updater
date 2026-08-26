from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import subprocess
import sys

import pytest

from services.ofs_technician_alert_service import AlertRuleSettings, TechnicianAlertClassifier
from services.ofs_technician_monitor_service import (
    MonitorSnapshot,
    MySQLTechnicianMonitorRepository,
    TechnicianMonitorService,
)


UTC = timezone.utc
WORK_DATE = date(2026, 8, 26)
SETTINGS = AlertRuleSettings(
    fallback_shift_start=time(8, 0),
    fallback_shift_end=time(19, 0),
    activation_tolerance_minutes=15,
    post_shift_tolerance_minutes=15,
    events_stale_seconds=180,
    activities_stale_seconds=1800,
    calendars_stale_seconds=5400,
    timezone_fallback="America/Sao_Paulo",
)


def now_utc(local_hour: int, local_minute: int = 0) -> datetime:
    return datetime(2026, 8, 26, local_hour + 3, local_minute, tzinfo=UTC)


def health(now: datetime, *, activities_status="ok"):
    naive = now.astimezone(UTC).replace(tzinfo=None)
    return {
        "events": {"status": "ok", "last_success_at": naive - timedelta(seconds=30)},
        "activities": {"status": activities_status, "last_success_at": naive - timedelta(minutes=5)},
        "calendars": {"status": "ok", "last_success_at": naive - timedelta(minutes=20)},
        "routes": {"status": "ok", "last_success_at": naive - timedelta(hours=10)},
    }


def hierarchy_row(resource_id, parent_id, resource_type, depth, name=None):
    return {
        "resource_id": resource_id,
        "parent_resource_id": parent_id,
        "resource_name": name or resource_id,
        "resource_type": resource_type,
        "status": "active",
        "timezone": "America/Sao_Paulo",
        "depth": depth,
        "root_resource_id": "02",
    }


def state_row(resource_id, **changes):
    row = {
        "work_date": WORK_DATE,
        "resource_id": resource_id,
        "route_state": "not_started",
        "route_started_at": None,
        "route_reactivated_at": None,
        "route_ended_at": None,
        "calendar_record_type": "working",
        "calendar_start_at": datetime(2026, 8, 26, 8, 0),
        "calendar_end_at": datetime(2026, 8, 26, 17, 0),
        "resource_timezone": "(UTC-03:00) Sao Paulo - Brasilia Time (BRT)",
        "resource_timezone_iana": "America/Sao_Paulo",
        "started_count": 0,
        "suspended_count": 0,
        "open_activity_count": 0,
    }
    row.update(changes)
    return row


class FakeRepository:
    def __init__(self, hierarchy, operational, health_rows):
        self.hierarchy = hierarchy
        self.operational = operational
        self.health_rows = health_rows
        self.calls = 0

    def load_snapshot(self, work_date, *, root_resource_id=None):
        self.calls += 1
        assert work_date == WORK_DATE
        assert root_resource_id == "02"
        return MonitorSnapshot(
            hierarchy=list(self.hierarchy),
            operational_rows=list(self.operational),
            health_by_source=dict(self.health_rows),
            query_metrics_ms={
                "hierarchy_query_ms": 1.0,
                "operational_query_ms": 2.0,
                "health_query_ms": 0.5,
            },
        )


def service_for(hierarchy, operational, health_rows):
    return TechnicianMonitorService(
        repository=FakeRepository(hierarchy, operational, health_rows),
        classifier=TechnicianAlertClassifier(SETTINGS),
    )


def basic_hierarchy():
    return [
        hierarchy_row("02", None, "ROOT", 0),
        hierarchy_row("GR1", "02", "GR", 1),
        hierarchy_row("BK1", "GR1", "BK", 2),
        hierarchy_row("T1", "BK1", "TCV", 3),
    ]


def test_summary_without_operational_rows_keeps_hierarchy_population_as_unknown():
    now = now_utc(12)
    service = service_for(basic_hierarchy(), [], health(now))
    payload, metrics = service.build_summary(WORK_DATE, root_resource_id="02", now=now)
    assert payload["data_available"] is False
    assert payload["total_technicians"] == 1
    assert payload["technicians_with_operational_state"] == 0
    assert payload["technicians_without_operational_state"] == 1
    assert payload["schedule"]["unknown"] == 1
    assert payload["routes"]["desconhecida"] == 1
    assert payload["operational"]["desconhecida"] == 1
    assert payload["integrity"]["desconhecida"] == 1
    assert payload["health"]["events_caught_up"] is None
    assert metrics["mysql_queries_total"] == 3


def test_summary_one_working_technician_waiting_route_after_tolerance():
    now = now_utc(8, 16)
    service = service_for(basic_hierarchy(), [state_row("T1")], health(now))
    payload, _ = service.build_summary(WORK_DATE, root_resource_id="02", now=now)
    assert payload["schedule"]["working"] == 1
    assert payload["routes"]["aguardando_ativacao"] == 1
    assert payload["operational"]["atencao"] == 1
    assert payload["alert_codes"]["ROUTE_NOT_STARTED"] == 1


def test_summary_on_call_and_non_working_remain_separate():
    now = now_utc(12)
    hierarchy = basic_hierarchy() + [hierarchy_row("T2", "BK1", "TCP", 3)]
    operational = [
        state_row("T1", calendar_record_type="on-call", calendar_start_at=None, calendar_end_at=None),
        state_row("T2", calendar_record_type="non-working", calendar_start_at=None, calendar_end_at=None),
    ]
    payload, _ = service_for(hierarchy, operational, health(now)).build_summary(
        WORK_DATE, root_resource_id="02", now=now
    )
    assert payload["schedule"]["on_call"] == 1
    assert payload["schedule"]["non_working"] == 1
    assert payload["routes"]["sem_escala"] == 2


def test_alert_and_degraded_integrity_are_independent_dimensions():
    now = now_utc(18)
    operational = [
        state_row(
            "T1",
            route_state="ended",
            route_started_at=datetime(2026, 8, 26, 8),
            route_ended_at=datetime(2026, 8, 26, 17),
            started_count=1,
            open_activity_count=1,
        )
    ]
    payload, _ = service_for(basic_hierarchy(), operational, health(now, activities_status="error")).build_summary(
        WORK_DATE, root_resource_id="02", now=now
    )
    assert payload["operational"]["alerta"] == 1
    assert payload["alert_codes"]["ACTIVITY_OPEN_AFTER_SHIFT"] == 1
    assert payload["integrity"]["dados_desatualizados"] == 1
    assert payload["started_count"] == 1
    assert payload["open_activity_count"] == 1


def test_only_degraded_integrity_does_not_become_operational_alert():
    now = now_utc(12)
    operational = [
        state_row("T1", route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2))
    ]
    payload, _ = service_for(basic_hierarchy(), operational, health(now, activities_status="error")).build_summary(
        WORK_DATE, root_resource_id="02", now=now
    )
    assert payload["integrity"]["dados_desatualizados"] == 1
    assert payload["operational"]["desconhecida"] == 1
    assert payload["operational"]["alerta"] == 0
    assert payload["operational"]["atencao"] == 0


def test_tree_parent_aggregates_multiple_levels_and_activity_counters():
    now = now_utc(12)
    hierarchy = basic_hierarchy() + [
        hierarchy_row("T2", "BK1", "TCP", 3),
        hierarchy_row("EMPTY", "GR1", "BK", 2),
    ]
    operational = [
        state_row(
            "T1",
            route_state="active",
            route_started_at=datetime(2026, 8, 26, 8, 2),
            started_count=2,
            open_activity_count=2,
        ),
        state_row(
            "T2",
            route_state="ended",
            route_started_at=datetime(2026, 8, 26, 8, 1),
            route_ended_at=datetime(2026, 8, 26, 11),
            suspended_count=1,
            open_activity_count=1,
        ),
    ]
    payload, metrics = service_for(hierarchy, operational, health(now)).build_tree(
        WORK_DATE, root_resource_id="02", now=now, mode="full"
    )
    root = payload["nodes"][0]
    assert root["resource_id"] == "02"
    assert root["aggregates"]["technician_count"] == 2
    assert root["aggregates"]["active_route_count"] == 1
    assert root["aggregates"]["ended_route_count"] == 1
    assert root["aggregates"]["started_count"] == 2
    assert root["aggregates"]["suspended_count"] == 1
    assert root["aggregates"]["open_activity_count"] == 3
    gr = root["children"][0]
    empty = next(child for child in gr["children"] if child["resource_id"] == "EMPTY")
    assert empty["aggregates"]["technician_count"] == 0
    assert metrics["payload_bytes"] > 0


def test_tree_defaults_to_children_mode_after_real_payload_measurement():
    now = now_utc(12)
    payload, _ = service_for(basic_hierarchy(), [state_row("T1")], health(now)).build_tree(
        WORK_DATE, root_resource_id="02", now=now
    )
    assert payload["mode"] == "children"
    assert payload["parent_id"] is None
    assert [node["resource_id"] for node in payload["nodes"]] == ["02"]


def test_tree_children_mode_returns_only_direct_children_with_descendant_aggregates():
    now = now_utc(12)
    hierarchy = basic_hierarchy() + [hierarchy_row("T2", "BK1", "TCP", 3)]
    operational = [
        state_row("T1", route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)),
        state_row("T2", route_state="ended", route_started_at=datetime(2026, 8, 26, 8), route_ended_at=datetime(2026, 8, 26, 11)),
    ]
    service = service_for(hierarchy, operational, health(now))
    payload, _ = service.build_tree(
        WORK_DATE, root_resource_id="02", now=now, mode="children", parent_id="BK1"
    )
    assert [node["resource_id"] for node in payload["nodes"]] == ["T1", "T2"]
    assert all("children" not in node for node in payload["nodes"])
    assert payload["parent_found"] is True


def test_only_problems_keeps_branch_to_problematic_technician():
    now = now_utc(8, 16)
    hierarchy = basic_hierarchy() + [
        hierarchy_row("GR2", "02", "GR", 1),
        hierarchy_row("T2", "GR2", "TCP", 2),
    ]
    operational = [
        state_row("T1"),
        state_row("T2", calendar_record_type="non-working", calendar_start_at=None, calendar_end_at=None),
    ]
    payload, _ = service_for(hierarchy, operational, health(now)).build_tree(
        WORK_DATE,
        root_resource_id="02",
        now=now,
        mode="children",
        parent_id="02",
        only_problems=True,
    )
    assert [node["resource_id"] for node in payload["nodes"]] == ["GR1"]
    assert payload["nodes"][0]["aggregates"]["attention_count"] == 1


def test_missing_parent_is_reported_without_crashing_tree():
    now = now_utc(12)
    hierarchy = basic_hierarchy() + [hierarchy_row("ORPHAN", "MISSING", "GR", 2)]
    payload, _ = service_for(hierarchy, [state_row("T1")], health(now)).build_tree(
        WORK_DATE, root_resource_id="02", now=now, mode="full"
    )
    assert any(item["resource_id"] == "ORPHAN" for item in payload["hierarchy_warnings"])
    assert {node["resource_id"] for node in payload["nodes"]} == {"02", "ORPHAN"}


def test_detail_mode_is_the_only_mode_that_emits_decision_reasons():
    now = now_utc(12)
    service = service_for(basic_hierarchy(), [state_row("T1")], health(now))
    compact, _ = service.build_tree(WORK_DATE, root_resource_id="02", now=now, mode="children", parent_id="BK1")
    detailed, _ = service.build_tree(WORK_DATE, root_resource_id="02", now=now, mode="children", parent_id="BK1", detail=True)
    assert "decision_reasons" not in compact["nodes"][0]
    assert "decision_reasons" in detailed["nodes"][0]


class FakeCursor:
    def __init__(self):
        self.execute_count = 0
        self.result = []

    def execute(self, sql, params=None):
        self.execute_count += 1
        if "FROM ofs_resource_hierarchy" in sql and "JOIN" not in sql:
            self.result = basic_hierarchy()
        elif "FROM ofs_technician_operational_state" in sql:
            self.result = [state_row("T1")]
        elif "FROM ofs_operational_sync_state" in sql:
            self.result = [{"source_name": "events", "status": "ok"}]
        else:
            self.result = []

    def fetchall(self):
        return list(self.result)

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.closed = False

    def cursor(self, dictionary=False):
        assert dictionary is True
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_mysql_monitor_repository_uses_exactly_three_bulk_queries():
    conn = FakeConnection()
    repo = MySQLTechnicianMonitorRepository(connection_factory=lambda: conn)
    snapshot = repo.load_snapshot(WORK_DATE, root_resource_id="02")
    assert len(snapshot.hierarchy) == 4
    assert len(snapshot.operational_rows) == 1
    assert conn.cursor_obj.execute_count == 3
    assert conn.closed is True


def test_monitor_source_has_no_oracle_ofs_or_requests_dependency():
    source = (Path(__file__).resolve().parents[1] / "services/ofs_technician_monitor_service.py").read_text(encoding="utf-8")
    route_source = (Path(__file__).resolve().parents[1] / "routes/ofs_technician_monitor_routes.py").read_text(encoding="utf-8")
    combined = source + route_source
    assert "OFSClient" not in combined
    assert "requests." not in combined
    assert "from requests" not in combined
    assert "/ofs" not in combined


def test_route_registry_registers_monitor_endpoints():
    source = (Path(__file__).resolve().parents[1] / "routes/__init__.py").read_text(encoding="utf-8")
    assert "init_ofs_technician_monitor_routes" in source
    assert "init_ofs_technician_monitor_routes(app)" in source


def test_monitor_probe_can_run_directly_from_tools_directory():
    project_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "tools/ofs_technician_monitor_probe.py", "--help"],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Mede as APIs locais do monitor de técnicos" in result.stdout
