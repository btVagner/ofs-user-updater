from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from services.ofs_operational_monitor_service import (
    MonitorScopeError,
    MonitorSourceUnavailable,
    SourceSnapshot,
    build_monitor_payload,
    resolve_scope,
)


WORK_DATE = date(2026, 9, 25)
NOW = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)


def hierarchy():
    return [
        {"resource_id": "02", "parent_resource_id": None, "resource_name": "Casa e Cliente", "resource_type": "GR", "status": "active", "timezone": "America/Sao_Paulo"},
        {"resource_id": "AREA", "parent_resource_id": "02", "resource_name": "Regional Sul", "resource_type": "BK", "status": "active", "timezone": "America/Sao_Paulo"},
        {"resource_id": "T1", "parent_resource_id": "AREA", "resource_name": "Técnico Um", "resource_type": "TCV", "status": "active", "timezone": "America/Sao_Paulo"},
        {"resource_id": "T2", "parent_resource_id": "AREA", "resource_name": "Técnico Dois", "resource_type": "TCV", "status": "active", "timezone": "America/Sao_Paulo"},
        {"resource_id": "T3", "parent_resource_id": "AREA", "resource_name": "Técnico Três", "resource_type": "TCV", "status": "active", "timezone": "America/Sao_Paulo"},
    ]


def states():
    return [
        {"resource_id": "T1", "route_state": "active", "route_started_at": datetime(2026, 9, 25, 8, 0), "route_reactivated_at": None, "route_ended_at": None, "calendar_start_at": datetime(2026, 9, 25, 8, 0), "calendar_end_at": datetime(2026, 9, 25, 18, 0), "resource_timezone_iana": "America/Sao_Paulo"},
        {"resource_id": "T2", "route_state": "active", "route_started_at": datetime(2026, 9, 25, 8, 10), "route_reactivated_at": None, "route_ended_at": None, "calendar_start_at": datetime(2026, 9, 25, 8, 0), "calendar_end_at": datetime(2026, 9, 25, 18, 0), "resource_timezone_iana": "America/Sao_Paulo"},
        {"resource_id": "T3", "route_state": "not_started", "route_started_at": None, "route_reactivated_at": None, "route_ended_at": None, "calendar_start_at": datetime(2026, 9, 25, 8, 0), "calendar_end_at": datetime(2026, 9, 25, 18, 0), "resource_timezone_iana": "America/Sao_Paulo"},
    ]


def activities():
    base = {"work_date": WORK_DATE, "record_type": "regular", "customer_name": None, "resource_timezone_iana": "America/Sao_Paulo"}
    return [
        {**base, "activity_id": "A1", "resource_id": "T1", "status": "started", "appt_number": "OS1", "activity_type": "INST", "start_time": datetime(2026, 9, 25, 9, 0), "duration_minutes": 60, "time_slot": "08:00-12:00", "is_black": 1},
        {**base, "activity_id": "A2", "resource_id": "T1", "status": "pending", "appt_number": "OS2", "activity_type": "MAN", "start_time": datetime(2026, 9, 25, 12, 30), "duration_minutes": 30, "time_slot": "08:00 - 12:00", "is_black": 0},
        {**base, "activity_id": "A3", "resource_id": "T1", "status": "pending", "appt_number": "OS3", "activity_type": "RET_INST", "start_time": datetime(2026, 9, 25, 12, 20), "duration_minutes": 30, "time_slot": "08:00-12:00", "is_black": 0},
        {**base, "activity_id": "I1", "resource_id": "T2", "status": "pending", "appt_number": None, "activity_type": "LUNCH", "record_type": "lunch", "start_time": datetime(2026, 9, 25, 12, 0), "duration_minutes": 60, "time_slot": None, "is_black": 0},
    ]


def test_build_payload_matches_plugin_operational_views():
    health = {
        "events": {"status": "ok", "last_success_at": datetime(2026, 9, 25, 14, 59)},
        "activities": {"status": "ok", "last_success_at": datetime(2026, 9, 25, 14, 59)},
        "calendars": {"status": "ok", "last_success_at": datetime(2026, 9, 25, 14, 30)},
        "routes": {"status": "ok", "last_success_at": datetime(2026, 9, 25, 12, 0)},
    }
    source = SourceSnapshot(hierarchy(), states(), activities(), health)
    payload = build_monitor_payload(source, resolve_scope("casa-cliente"), WORK_DATE, now=NOW)

    assert payload["technicians_count"] == 3
    assert [row["id"] for row in payload["late_candidates"]] == ["A1"]
    assert [row["resource_id"] for row in payload["idle"]] == ["T2"]
    assert [row["resource_id"] for row in payload["not_started_candidates"]] == ["T3"]
    assert {row["id"] for row in payload["slot"]} == {"A2", "A3"}
    assert next(row for row in payload["slot"] if row["id"] == "A3")["is_withdrawal"] is True
    assert [row["id"] for row in payload["black"]] == ["A1"]
    assert payload["late_candidates"][0]["area"].endswith("Regional Sul")


def test_invalid_scope_is_rejected_before_database_access():
    with pytest.raises(MonitorScopeError):
        resolve_scope("macro-inventada")


def test_empty_hierarchy_is_reported_as_source_unavailable():
    with pytest.raises(MonitorSourceUnavailable):
        build_monitor_payload(SourceSnapshot([], [], [], {}), resolve_scope("casa-cliente"), WORK_DATE, now=NOW)
