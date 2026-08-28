from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

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
    AlertRuleSettings,
    MySQLTechnicianAlertReadRepository,
    TechnicianAlertClassifier,
    resolve_technician_timezone,
)
from services.ofs_technician_operational_service import normalize_calendar_item


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


def now_utc(local_hour: int, local_minute: int = 0, *, local_day: int = 26) -> datetime:
    # America/Sao_Paulo is UTC-03 on the validated 2026 date.
    return datetime(2026, 8, local_day, local_hour + 3, local_minute, tzinfo=UTC)


def healthy(now: datetime, *, routes_age_hours: int = 10, caught_up=True):
    naive = now.astimezone(UTC).replace(tzinfo=None)
    return {
        "events": {
            "status": "ok",
            "last_success_at": naive - timedelta(seconds=30),
            "caught_up": caught_up,
        },
        "activities": {"status": "ok", "last_success_at": naive - timedelta(minutes=5)},
        "calendars": {"status": "ok", "last_success_at": naive - timedelta(minutes=20)},
        "routes": {"status": "ok", "last_success_at": naive - timedelta(hours=routes_age_hours)},
    }


def base_row(**changes):
    row = {
        "work_date": WORK_DATE,
        "resource_id": "T1",
        "resource_timezone": "(UTC-03:00) Sao Paulo - Brasilia Time (BRT)",
        "resource_timezone_iana": "America/Sao_Paulo",
        "calendar_record_type": "working",
        "calendar_start_at": datetime(2026, 8, 26, 8, 0),
        "calendar_end_at": datetime(2026, 8, 26, 17, 0),
        "route_state": "not_started",
        "route_started_at": None,
        "route_reactivated_at": None,
        "route_ended_at": None,
        "started_count": 0,
        "suspended_count": 0,
        "open_activity_count": 0,
        "updated_at": datetime(2026, 8, 26, 8, 0),
    }
    row.update(changes)
    return row


def classify(row=None, now=None, health=None):
    row = row or base_row()
    now = now or now_utc(12)
    return TechnicianAlertClassifier(SETTINGS).classify_one(row, health or healthy(now), now=now)


@pytest.mark.parametrize(
    "local_hour,local_minute,expected_severity",
    [
        (7, 59, SEVERITY_NORMAL),
        (8, 0, SEVERITY_NORMAL),
        (8, 14, SEVERITY_NORMAL),
        (8, 15, SEVERITY_ATTENTION),
    ],
)
def test_route_not_started_boundary(local_hour, local_minute, expected_severity):
    result = classify(now=now_utc(local_hour, local_minute))
    assert result["schedule_state"] == SCHEDULE_WORKING
    assert result["route_state"] == ROUTE_WAITING
    assert result["operational_severity"] == expected_severity
    assert (ALERT_ROUTE_NOT_STARTED in result["alert_codes"]) is (expected_severity == SEVERITY_ATTENTION)


def test_afternoon_shift_uses_real_calendar_times():
    row = base_row(
        calendar_start_at=datetime(2026, 8, 26, 13, 0),
        calendar_end_at=datetime(2026, 8, 26, 18, 0),
    )
    before = classify(row=row, now=now_utc(12, 30))
    late = classify(row=row, now=now_utc(13, 16))
    assert before["operational_severity"] == SEVERITY_NORMAL
    assert late["alert_codes"] == [ALERT_ROUTE_NOT_STARTED]


@pytest.mark.parametrize("record_type", ["extra_working", "extra-working", "extraworking"])
def test_extra_working_is_treated_as_working_schedule(record_type):
    result = classify(row=base_row(calendar_record_type=record_type), now=now_utc(12))
    assert result["schedule_state"] == SCHEDULE_WORKING
    assert result["route_state"] == ROUTE_WAITING


def test_non_working_never_generates_route_delay():
    result = classify(row=base_row(calendar_record_type="non-working", calendar_start_at=None, calendar_end_at=None), now=now_utc(15))
    assert result["schedule_state"] == SCHEDULE_NON_WORKING
    assert result["route_state"] == ROUTE_NO_SCHEDULE
    assert result["alert_codes"] == []
    assert result["operational_severity"] == SEVERITY_NORMAL


def test_on_call_is_preserved_and_does_not_imply_activation_obligation():
    result = classify(row=base_row(calendar_record_type="on-call", calendar_start_at=datetime(2026, 8, 26, 13), calendar_end_at=datetime(2026, 8, 26, 18)), now=now_utc(15))
    assert result["schedule_state"] == SCHEDULE_ON_CALL
    assert result["route_state"] == ROUTE_NO_SCHEDULE
    assert result["alert_codes"] == []


def test_d06_normalizer_preserves_on_call_semantic_even_when_record_type_is_working():
    row = normalize_calendar_item({
        "resourceId": "T3",
        "date": "2026-08-26",
        "on-call": {"recordType": "working", "workTimeStart": "13:00", "workTimeEnd": "18:00"},
    })
    assert row["calendar_record_type"] == "on-call"


def test_active_route_is_detected_from_latest_start():
    result = classify(row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)), now=now_utc(12))
    assert result["route_state"] == ROUTE_ACTIVE
    assert result["alert_codes"] == []


def test_ended_route_is_detected_when_end_is_after_start():
    result = classify(row=base_row(route_state="ended", route_started_at=datetime(2026, 8, 26, 8, 2), route_ended_at=datetime(2026, 8, 26, 17, 2)), now=now_utc(17, 10))
    assert result["route_state"] == ROUTE_ENDED


def test_reactivation_after_end_makes_route_active_again():
    result = classify(row=base_row(
        route_state="active",
        route_started_at=datetime(2026, 8, 26, 8, 2),
        route_ended_at=datetime(2026, 8, 26, 12, 0),
        route_reactivated_at=datetime(2026, 8, 26, 12, 30),
    ), now=now_utc(13))
    assert result["route_state"] == ROUTE_ACTIVE


def test_post_shift_active_route_without_open_activity_is_attention():
    result = classify(row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)), now=now_utc(17, 16))
    assert result["alert_codes"] == [ALERT_ROUTE_ACTIVE_AFTER_SHIFT]
    assert result["operational_severity"] == SEVERITY_ATTENTION


@pytest.mark.parametrize("counter", ["started_count", "suspended_count"])
def test_post_shift_open_activity_is_high_severity(counter):
    row = base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2), **{counter: 1})
    result = classify(row=row, now=now_utc(17, 16))
    assert result["alert_codes"] == [ALERT_ACTIVITY_OPEN_AFTER_SHIFT]
    assert result["operational_severity"] == SEVERITY_ALERT


@pytest.mark.parametrize("counter", ["started_count", "suspended_count"])
def test_open_activity_after_shift_survives_even_if_route_is_ended(counter):
    row = base_row(
        route_state="ended",
        route_started_at=datetime(2026, 8, 26, 8, 2),
        route_ended_at=datetime(2026, 8, 26, 17, 0),
        **{counter: 1},
    )
    result = classify(row=row, now=now_utc(17, 16))
    assert result["route_state"] == ROUTE_ENDED
    assert result["alert_codes"] == [ALERT_ACTIVITY_OPEN_AFTER_SHIFT]


def test_missing_calendar_with_healthy_source_is_unknown_not_route_delay():
    result = classify(row=base_row(calendar_record_type=None, calendar_start_at=None, calendar_end_at=None), now=now_utc(10))
    assert result["schedule_state"] == SCHEDULE_UNKNOWN
    assert result["route_state"] == ROUTE_UNKNOWN
    assert result["alert_codes"] == []
    assert result["integrity_state"] == INTEGRITY_UNKNOWN
    assert result["operational_severity"] == SEVERITY_UNKNOWN
    assert "CALENDAR_RECORD_UNKNOWN" in result["integrity_codes"]


def test_calendar_stale_suppresses_absence_based_route_alert():
    now = now_utc(10)
    health = healthy(now)
    health["calendars"]["last_success_at"] = now.replace(tzinfo=None) - timedelta(hours=2)
    result = classify(now=now, health=health)
    assert result["integrity_state"] == INTEGRITY_STALE
    assert result["alert_codes"] == []
    assert result["operational_severity"] == SEVERITY_UNKNOWN


@pytest.mark.parametrize("status", ["error", "failure"])
def test_events_failure_suppresses_absence_based_route_alert(status):
    now = now_utc(10)
    health = healthy(now)
    health["events"]["status"] = status
    result = classify(now=now, health=health)
    assert result["integrity_state"] == INTEGRITY_STALE
    assert ALERT_ROUTE_NOT_STARTED not in result["alert_codes"]
    assert result["operational_severity"] == SEVERITY_UNKNOWN


def test_events_not_caught_up_is_integrity_degradation_and_suppresses_absence_alert():
    now = now_utc(10)
    health = healthy(now, caught_up=False)
    result = classify(now=now, health=health)
    assert result["integrity_state"] == INTEGRITY_STALE
    assert result["freshness"]["events"]["caught_up"] is False
    assert ALERT_ROUTE_NOT_STARTED not in result["alert_codes"]


def test_activities_failure_marks_integrity_degraded_without_erasing_known_dimensions():
    now = now_utc(12)
    health = healthy(now)
    health["activities"]["status"] = "error"
    result = classify(
        row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)),
        now=now,
        health=health,
    )
    assert result["route_state"] == ROUTE_ACTIVE
    assert result["integrity_state"] == INTEGRITY_STALE
    assert result["operational_severity"] == SEVERITY_UNKNOWN


def test_events_stale_suppresses_route_active_after_shift_because_end_may_be_missing():
    now = now_utc(18)
    health = healthy(now)
    health["events"]["last_success_at"] = now.replace(tzinfo=None) - timedelta(minutes=10)
    result = classify(row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)), now=now, health=health)
    assert result["integrity_state"] == INTEGRITY_STALE
    assert ALERT_ROUTE_ACTIVE_AFTER_SHIFT not in result["alert_codes"]
    assert result["operational_severity"] == SEVERITY_UNKNOWN


def test_activities_stale_marks_integrity_degraded():
    now = now_utc(12)
    health = healthy(now)
    health["activities"]["last_success_at"] = now.replace(tzinfo=None) - timedelta(hours=1)
    result = classify(row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)), now=now, health=health)
    assert result["integrity_state"] == INTEGRITY_STALE
    assert "ACTIVITIES_STALE" in result["integrity_codes"]


def test_old_routes_health_does_not_make_all_technicians_stale_when_events_are_healthy():
    now = now_utc(12)
    result = classify(
        row=base_row(route_state="active", route_started_at=datetime(2026, 8, 26, 8, 2)),
        now=now,
        health=healthy(now, routes_age_hours=36, caught_up=True),
    )
    assert result["freshness"]["routes"]["state"] == INTEGRITY_OK
    assert result["freshness"]["routes"]["threshold_seconds"] is None
    assert result["freshness"]["events"]["caught_up"] is True
    assert result["integrity_state"] == INTEGRITY_OK


def test_old_individual_updated_at_is_not_used_as_global_freshness():
    now = now_utc(12)
    row = base_row(
        route_state="active",
        route_started_at=datetime(2026, 8, 26, 8, 2),
        updated_at=datetime(2026, 8, 26, 1, 0),
    )
    result = classify(row=row, now=now, health=healthy(now))
    assert result["integrity_state"] == INTEGRITY_OK
    assert result["operational_severity"] == SEVERITY_NORMAL


def test_positive_operational_alert_is_preserved_with_integrity_degradation():
    now = now_utc(18)
    health = healthy(now)
    health["activities"]["status"] = "error"
    row = base_row(started_count=1, route_state="ended", route_started_at=datetime(2026, 8, 26, 8), route_ended_at=datetime(2026, 8, 26, 17))
    result = classify(row=row, now=now, health=health)
    assert result["alert_codes"] == [ALERT_ACTIVITY_OPEN_AFTER_SHIFT]
    assert result["operational_severity"] == SEVERITY_ALERT
    assert result["integrity_state"] == INTEGRITY_STALE


def test_iana_timezone_is_preferred():
    result = classify(row=base_row(resource_timezone_iana="America/Sao_Paulo"), now=now_utc(12))
    assert result["timezone"] == "America/Sao_Paulo"
    assert result["timezone_source"] == "RESOURCE_IANA"


def test_descriptive_timezone_offset_is_safe_fallback():
    row = base_row(resource_timezone_iana=None, resource_timezone="(UTC-03:00) Sao Paulo - Brasilia Time (BRT)")
    result = classify(row=row, now=now_utc(12))
    assert result["timezone"] == "UTC-03:00"
    assert result["timezone_source"] == "RESOURCE_OFFSET"
    assert result["local_now"].startswith("2026-08-26T12:00:00-03:00")


def test_invalid_timezone_falls_back_to_central_timezone():
    row = base_row(resource_timezone_iana="Not/A_Zone", resource_timezone="invalid")
    result = classify(row=row, now=now_utc(12))
    assert result["timezone"] == "America/Sao_Paulo"
    assert result["timezone_source"] == "FALLBACK"
    assert "TIMEZONE_FALLBACK" in result["decision_reasons"]


def test_utc_day_turn_does_not_break_local_operational_day():
    # 27/08 01:30 UTC is still 26/08 22:30 in Sao Paulo.
    now = datetime(2026, 8, 27, 1, 30, tzinfo=UTC)
    row = base_row(started_count=1, route_state="ended", route_started_at=datetime(2026, 8, 26, 8), route_ended_at=datetime(2026, 8, 26, 17))
    result = classify(row=row, now=now, health=healthy(now))
    assert result["local_now"].startswith("2026-08-26T22:30:00")
    assert result["alert_codes"] == [ALERT_ACTIVITY_OPEN_AFTER_SHIFT]


def test_row_from_different_local_operational_day_does_not_emit_temporal_alert():
    now = datetime(2026, 8, 27, 5, 0, tzinfo=UTC)  # 02:00 local on 27/08
    row = base_row(started_count=1)
    result = classify(row=row, now=now, health=healthy(now))
    assert result["alert_codes"] == []
    assert result["integrity_state"] == INTEGRITY_UNKNOWN
    assert result["operational_severity"] == SEVERITY_UNKNOWN


def test_working_schedule_with_missing_exact_times_uses_centralized_business_fallback():
    result = classify(row=base_row(calendar_start_at=None, calendar_end_at=None), now=now_utc(8, 16))
    assert result["schedule_state"] == SCHEDULE_WORKING
    assert result["schedule_mode"] == "FALLBACK_HORARIO_NEGOCIO"
    assert result["effective_shift_start"].startswith("2026-08-26T08:00:00")
    assert result["effective_shift_end"].startswith("2026-08-26T19:00:00")
    assert result["alert_codes"] == [ALERT_ROUTE_NOT_STARTED]


def test_inconsistent_route_end_without_any_start_is_unknown():
    result = classify(row=base_row(route_state="ended", route_ended_at=datetime(2026, 8, 26, 9, 0)), now=now_utc(10))
    assert result["route_state"] == ROUTE_UNKNOWN
    assert result["integrity_state"] == INTEGRITY_UNKNOWN
    assert "ROUTE_FACTS_INCONSISTENT" in result["integrity_codes"]


def test_classifier_batch_does_not_query_or_call_external_services():
    classifier = TechnicianAlertClassifier(SETTINGS)
    now = now_utc(12)
    rows = [base_row(resource_id=f"T{i}", route_state="active", route_started_at=datetime(2026, 8, 26, 8)) for i in range(1333)]
    results = classifier.classify_batch(rows, healthy(now), now=now)
    assert len(results) == 1333
    assert all(result["integrity_state"] == INTEGRITY_OK for result in results)


class FakeCursor:
    def __init__(self):
        self.execute_count = 0
        self._result = []

    def execute(self, sql, params=None):
        self.execute_count += 1
        if "FROM ofs_technician_operational_state" in sql:
            self._result = [base_row()]
        elif "FROM ofs_operational_sync_state" in sql:
            self._result = [
                {"source_name": "events", "status": "ok", "last_success_at": datetime(2026, 8, 26, 15)},
                {"source_name": "activities", "status": "ok", "last_success_at": datetime(2026, 8, 26, 15)},
                {"source_name": "calendars", "status": "ok", "last_success_at": datetime(2026, 8, 26, 15)},
            ]
        else:
            self._result = []

    def fetchall(self):
        return list(self._result)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.closed = False

    def cursor(self, dictionary=False):
        assert dictionary is True
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_mysql_reader_uses_two_bulk_queries_not_n_plus_one():
    conn = FakeConn()
    repo = MySQLTechnicianAlertReadRepository(connection_factory=lambda: conn)
    rows, health = repo.load_snapshot(WORK_DATE, root_resource_id="02")
    assert len(rows) == 1
    assert set(health) == {"events", "activities", "calendars"}
    assert conn.cursor_obj.execute_count == 2
    assert conn.closed is True


def test_alert_service_source_contains_no_ofs_client_or_requests_dependency():
    source = (Path(__file__).resolve().parents[1] / "services/ofs_technician_alert_service.py").read_text(encoding="utf-8")
    assert "OFSClient" not in source
    assert "import requests" not in source
    assert ".get_events(" not in source
    assert ".get_route(" not in source
    assert ".get_activities(" not in source
    assert ".get_calendars(" not in source
