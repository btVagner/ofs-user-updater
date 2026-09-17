from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from services.ofs_technician_operational_service import (
    ACTIVITY_EVENTS,
    ACTIVITY_FIELDS,
    CURSOR_KEY,
    MySQLOperationalRepository,
    OFSOperationalAPI,
    OperationalAPIError,
    OperationalAlreadyRunning,
    OperationalSettings,
    TechnicianOperationalCollector,
    event_should_apply,
    mysql_operational_lock,
    normalize_activity,
    normalize_calendar_item,
    normalize_route_baseline,
    parse_activity_event,
    parse_route_event,
    retention_cutoff,
    is_retained_work_date,
)


class DummyAuth:
    pass


class DummyClient:
    base_url = "https://example.invalid/rest/ofscCore/v1"
    auth = DummyAuth()


class FakeResponse:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.ok = 200 <= status < 300
        self.content = b"{}" if status != 204 else b""

    def json(self):
        return self._payload


class FakeRepo:
    def __init__(self):
        self.cursor = None
        self.created_subscriptions = []
        self.health = []
        self.technicians = [
            {"resource_id": "T1", "resource_type": "TCV", "timezone": "BRT"},
            {"resource_id": "T2", "resource_type": "TCP", "timezone": "BRT"},
        ]
        self.rows_ensured = []
        self.calendar_rows = []
        self.activity_rows = []
        self.route_rows = []
        self.baseline_completed = False
        self.purges = []
        self.event_pages = []
        self.fail_event_commit = False

    def get_technicians(self, root):
        return list(self.technicians)

    def get_cursor(self, cursor_key=CURSOR_KEY):
        return dict(self.cursor) if self.cursor else None

    def save_new_subscription(self, subscription_id, next_page, now, cursor_key=CURSOR_KEY):
        self.cursor = {
            "cursor_key": cursor_key,
            "subscription_id": subscription_id,
            "next_page": next_page,
            "baseline_completed_at": None,
        }
        self.created_subscriptions.append((subscription_id, next_page))

    def mark_baseline_completed(self, now, cursor_key=CURSOR_KEY):
        self.baseline_completed = True
        if self.cursor:
            self.cursor["baseline_completed_at"] = now

    def ensure_technician_rows(self, technicians, work_date, now):
        self.rows_ensured.append((work_date, tuple(t["resource_id"] for t in technicians)))

    def apply_calendars(self, rows, technician_ids, now, work_date=None):
        self.calendar_rows = list(rows)
        return len([r for r in rows if r.get("resourceId") in technician_ids])

    def replace_activities_for_date(self, work_date, activities, technician_ids, reconciled_at):
        self.activity_rows = list(activities)
        return {"activities": len(activities), "deleted": 0, "affected_resources": 1}

    def apply_route_baseline(self, route_rows, now):
        self.route_rows = list(route_rows)
        return len(route_rows)

    def update_health(self, source, **kwargs):
        self.health.append((source, kwargs))

    def purge_retention(self, today, retention_days=7):
        self.purges.append((today, retention_days))
        return {"cutoff": retention_cutoff(today, retention_days).isoformat(), "activities_deleted": 0, "technicians_deleted": 0}

    def apply_event_page(self, subscription_id, current_page, next_page, events, now, technician_ids, **kwargs):
        self.event_pages.append((current_page, next_page, list(events)))
        if self.fail_event_commit:
            raise RuntimeError("simulated rollback")
        assert self.cursor["subscription_id"] == subscription_id
        assert self.cursor["next_page"] == current_page
        self.cursor["next_page"] = next_page
        return {
            "applied": len(events),
            "ignored": 0,
            "unsafe": 0,
            "events": len(events),
            "page_changed": next_page != current_page,
        }

    def operational_metrics(self, work_date=None):
        return {"technician_rows": 2, "activity_rows": len(self.activity_rows), "table_sizes": {}}


class FakeAPI:
    def __init__(self):
        self.subscription_exists = False
        self.created = 0
        self.events = []
        self.calendars_calls = 0
        self.activities_calls = 0
        self.route_calls = 0

    def list_subscription(self, subscription_id):
        return {"subscriptionId": subscription_id} if self.subscription_exists else None

    def create_subscription(self):
        self.created += 1
        self.subscription_exists = True
        return "sub-secret-id", "opaque,marker-01"

    def get_calendars(self, root, work_date):
        self.calendars_calls += 1
        return ([
            {"resourceId": "T1", "date": work_date.isoformat(), "regular": {"recordType": "working", "workTimeStart": "08:00", "workTimeEnd": "17:00"}},
            {"resourceId": "T2", "date": work_date.isoformat(), "regular": {"recordType": "non-working", "nonWorkingReason": "FOLGA"}},
        ], 1)

    def get_activities(self, root, work_date):
        self.activities_calls += 1
        return ([
            {"activityId": 1, "resourceId": "T1", "date": work_date.isoformat(), "status": "pending", "activityType": "SUP"},
        ], 1)

    def get_route(self, resource_id, work_date):
        self.route_calls += 1
        payload = {"resourceId": resource_id, "date": work_date.isoformat(), "items": []}
        if resource_id == "T1":
            payload["routeStartTime"] = f"{work_date.isoformat()} 08:01"
        return payload, 1

    def get_events(self, subscription_id, page):
        if self.events:
            return self.events.pop(0)
        return [], page


def test_normalize_activity_compact_state():
    row = normalize_activity({
        "activityId": 123,
        "resourceId": "T1",
        "date": "2026-08-26",
        "status": "STARTED",
        "apptNumber": "ABC",
        "activityType": "SUP",
        "resourceTimeZoneIANA": "America/Sao_Paulo",
        "ignoredHugeField": "x" * 10000,
    })
    assert row["activity_id"] == "123"
    assert row["status"] == "started"
    assert row["resource_timezone_iana"] == "America/Sao_Paulo"
    assert "ignoredHugeField" not in row


def test_calendar_working_and_non_working():
    working = normalize_calendar_item({
        "resourceId": "T1",
        "date": "2026-08-26",
        "regular": {"recordType": "working", "workTimeStart": "08:00", "workTimeEnd": "17:00"},
    })
    assert working["calendar_start_at"] == datetime(2026, 8, 26, 8, 0)
    assert working["calendar_end_at"] == datetime(2026, 8, 26, 17, 0)

    non_working = normalize_calendar_item({
        "resourceId": "T2",
        "date": "2026-08-26",
        "regular": {"recordType": "non-working", "nonWorkingReason": "FOLGA"},
    })
    assert non_working["calendar_record_type"] == "non-working"
    assert non_working["calendar_start_at"] is None
    assert non_working["non_working_reason"] == "FOLGA"


def test_route_baseline_states():
    d = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 12, 0)
    not_started = normalize_route_baseline("T1", d, {}, reconciled_at=now)
    active = normalize_route_baseline("T1", d, {"routeStartTime": "2026-08-26 08:02"}, reconciled_at=now)
    ended = normalize_route_baseline("T1", d, {"routeStartTime": "2026-08-26 08:02", "routeEndTime": "2026-08-26 17:03"}, reconciled_at=now)
    assert not_started["route_state"] == "not_started"
    assert active["route_state"] == "active"
    assert ended["route_state"] == "ended"


def test_route_baseline_preserves_local_wall_clock_with_offset():
    row = normalize_route_baseline(
        "T1",
        date(2026, 9, 17),
        {
            "routeStartTime": "2026-09-17T08:07:00-03:00",
            "routeReactivationTime": "2026-09-17T09:15:00-03:00",
            "routeEndTime": "2026-09-17T18:02:00-03:00",
        },
        reconciled_at=datetime(2026, 9, 17, 21, 0),
    )
    assert row["route_started_at"] == datetime(2026, 9, 17, 8, 7)
    assert row["route_reactivated_at"] == datetime(2026, 9, 17, 9, 15)
    assert row["route_ended_at"] == datetime(2026, 9, 17, 18, 2)


@pytest.mark.parametrize(
    "event_type,expected",
    [
        ("activityStarted", "started"),
        ("activityTravelStarted", "enroute"),
        ("activitySuspended", "suspended"),
        ("activityCompleted", "completed"),
        ("activityNotDone", "notdone"),
        ("activityCanceled", "cancelled"),
    ],
)
def test_activity_event_status_fallbacks(event_type, expected):
    op = parse_activity_event({
        "eventType": event_type,
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 7, "resourceId": "T1", "date": "2026-08-26"},
    })
    assert op["status"] == expected


def test_activity_travel_stopped_requires_destination_status():
    op = parse_activity_event({
        "eventType": "activityTravelStopped",
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 7, "resourceId": "T1", "date": "2026-08-26"},
    })
    assert op["kind"] == "unsafe_activity_event"


def test_activity_moved_uses_original_and_destination():
    op = parse_activity_event({
        "eventType": "activityMoved",
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 9, "resourceId": "T1", "date": "2026-08-26", "status": "pending"},
        "activityChanges": {"resourceId": "T2", "date": "2026-08-27"},
    })
    assert op["original_resource_id"] == "T1"
    assert op["original_work_date"] == date(2026, 8, 26)
    assert op["resource_id"] == "T2"
    assert op["work_date"] == date(2026, 8, 27)
    assert op["status"] == "pending"


def test_event_duplicate_and_out_of_order_policy():
    t = datetime(2026, 8, 26, 12, 0)
    assert event_should_apply(None, None, t, "a")
    assert not event_should_apply(t, "same", t, "same")
    assert event_should_apply(t, "a", t, "b")  # evento distinto no mesmo segundo
    assert not event_should_apply(t, "a", datetime(2026, 8, 26, 11, 59, 59), "older")


def test_route_event_parser_is_dimension_specific():
    op = parse_route_event({
        "eventType": "routeActivated",
        "time": "2026-08-26 11:02:00",
        "routeDetails": {"resourceId": "T1", "date": "2026-08-26"},
        "routeChanges": {"activated": "2026-08-26 08:02", "calendarTimeFrom": "08:00", "calendarTimeTo": "17:00", "timeZone": "BRT"},
    })
    assert op["route_state"] == "active"
    assert op["work_date"] == date(2026, 8, 26)
    assert op["calendar_start_at"] == datetime(2026, 8, 26, 8, 0)


@pytest.mark.parametrize(
    "event_type,change_key,result_key,value,expected",
    [
        ("routeReactivated", "reactivated", "route_reactivated_at", "2026-09-17T09:15:00-03:00", datetime(2026, 9, 17, 9, 15)),
        ("routeDeactivated", "deactivated", "route_ended_at", "2026-09-17T18:02:00-03:00", datetime(2026, 9, 17, 18, 2)),
        ("routeActivated", "activated", "route_started_at", "2026-09-17 08:02", datetime(2026, 9, 17, 8, 2)),
    ],
)
def test_route_event_preserves_local_wall_clock(event_type, change_key, result_key, value, expected):
    op = parse_route_event(
        {
            "eventType": event_type,
            "time": "2026-09-17T11:00:00Z",
            "routeDetails": {"resourceId": "T1", "date": "2026-09-17"},
            "routeChanges": {change_key: value},
        }
    )
    assert op[result_key] == expected
    assert op["event_at"] == datetime(2026, 9, 17, 11, 0)


def test_route_activated_keeps_event_at_utc_and_started_at_local():
    op = parse_route_event(
        {
            "eventType": "routeActivated",
            "time": "2026-09-17T11:00:00Z",
            "routeDetails": {"resourceId": "7671", "date": "2026-09-17"},
            "routeChanges": {"activated": "2026-09-17T08:00:00-03:00"},
        }
    )
    assert op["event_at"] == datetime(2026, 9, 17, 11, 0)
    assert op["route_started_at"] == datetime(2026, 9, 17, 8, 0)


def test_retention_is_exactly_today_minus_six():
    assert retention_cutoff(date(2026, 8, 26), 7) == date(2026, 8, 20)


def test_retained_work_date_excludes_future_and_expired_dates():
    today = date(2026, 8, 26)
    assert is_retained_work_date(date(2026, 8, 26), today, 7)
    assert is_retained_work_date(date(2026, 8, 20), today, 7)
    assert not is_retained_work_date(date(2026, 8, 27), today, 7)
    assert not is_retained_work_date(date(2026, 8, 19), today, 7)


def test_subscription_cursor_persisted_before_baseline_and_opaque():
    repo = FakeRepo()
    api = FakeAPI()
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    result = collector.ensure_subscription()
    assert result["created"] is True
    assert repo.cursor["next_page"] == "opaque,marker-01"
    assert repo.baseline_completed is False


def test_baseline_then_events_closes_race_window():
    repo = FakeRepo()
    api = FakeAPI()
    api.events = [
        ([{
            "eventType": "activityStarted",
            "time": "2026-08-26 12:00:00",
            "activityDetails": {"activityId": 1, "resourceId": "T1", "date": "2026-08-26", "status": "started"},
        }], "opaque,marker-02"),
        ([], "opaque,marker-02"),
    ]
    collector = TechnicianOperationalCollector(
        api=api,
        repository=repo,
        settings=OperationalSettings(route_workers=2),
        root_resource_id="02",
    )
    result = collector.run_baseline(date(2026, 8, 26))
    assert repo.created_subscriptions[0][1] == "opaque,marker-01"
    assert repo.baseline_completed is True
    assert result["events_after_baseline"]["polls"] == 2
    assert repo.cursor["next_page"] == "opaque,marker-02"
    assert api.route_calls == 2


def test_event_cursor_does_not_advance_when_commit_fails():
    repo = FakeRepo()
    api = FakeAPI()
    repo.cursor = {"subscription_id": "s1", "next_page": "opaque:start"}
    repo.fail_event_commit = True
    api.subscription_exists = True
    api.get_events = lambda subscription_id, page: ([{"eventType": "ignored"}], "opaque:next")
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    with pytest.raises(RuntimeError, match="simulated rollback"):
        collector.poll_events_once()
    assert repo.cursor["next_page"] == "opaque:start"


def test_empty_event_poll_is_successful_and_keeps_cursor():
    repo = FakeRepo()
    api = FakeAPI()
    repo.cursor = {"subscription_id": "s1", "next_page": "opaque:still"}
    api.subscription_exists = True
    api.get_events = lambda subscription_id, page: ([], page)
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    result = collector.poll_events_once()
    assert result["events"] == 0
    assert result["page_changed"] is False
    assert repo.cursor["next_page"] == "opaque:still"
    assert any(source == "events" and data.get("status") == "ok" for source, data in repo.health)


def test_health_updates_even_when_technicians_do_not_change():
    repo = FakeRepo()
    api = FakeAPI()
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    collector.reconcile_activities(date(2026, 8, 26))
    assert any(source == "activities" and data.get("status") == "ok" and data.get("success_at") for source, data in repo.health)


def test_request_retries_429_then_succeeds(monkeypatch):
    calls = []
    responses = [FakeResponse(429, {"detail": "busy"}, {"Retry-After": "0"}), FakeResponse(200, {"items": []})]

    def fake_request(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr(requests, "request", fake_request)
    api = OFSOperationalAPI(
        client=DummyClient(),
        settings=OperationalSettings(request_retries=2, backoff_base_seconds=0, request_timeout_seconds=1),
        sleep=lambda _: None,
    )
    assert api._request("GET", "activities") == {"items": []}
    assert len(calls) == 2


def test_request_timeout_retries_then_raises(monkeypatch):
    calls = 0

    def fake_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise requests.Timeout("timeout Authorization=secret")

    monkeypatch.setattr(requests, "request", fake_request)
    api = OFSOperationalAPI(
        client=DummyClient(),
        settings=OperationalSettings(request_retries=2, backoff_base_seconds=0, request_timeout_seconds=1),
        sleep=lambda _: None,
    )
    with pytest.raises(OperationalAPIError) as exc:
        api._request("GET", "events")
    assert calls == 2
    assert "secret" not in str(exc.value)
    assert "<redacted>" in str(exc.value)


def test_request_401_is_not_retried(monkeypatch):
    calls = 0

    def fake_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        return FakeResponse(401, {"detail": "unauthorized"})

    monkeypatch.setattr(requests, "request", fake_request)
    api = OFSOperationalAPI(client=DummyClient(), settings=OperationalSettings(request_retries=4), sleep=lambda _: None)
    with pytest.raises(OperationalAPIError) as exc:
        api._request("GET", "events")
    assert exc.value.status_code == 401
    assert calls == 1


def test_mysql_lock_rejects_second_instance():
    class Cursor:
        def __init__(self):
            self.closed = False
        def execute(self, sql, params):
            self.sql = sql
        def fetchone(self):
            return (0,)
        def close(self):
            self.closed = True
    class Conn:
        def __init__(self):
            self.cur = Cursor()
            self.closed = False
        def cursor(self):
            return self.cur
        def close(self):
            self.closed = True
    conn = Conn()
    with pytest.raises(OperationalAlreadyRunning):
        with mysql_operational_lock(connection_factory=lambda: conn):
            pass
    assert conn.closed is True


def test_sql_has_required_upserts_indexes_and_rollback():
    root = Path(__file__).resolve().parents[1]
    apply_sql = (root / "database/sql/20260826_ofs_technician_operational_apply.sql").read_text(encoding="utf-8")
    rollback_sql = (root / "database/sql/20260826_ofs_technician_operational_rollback.sql").read_text(encoding="utf-8")
    assert "PRIMARY KEY (work_date, resource_id)" in apply_sql
    assert "PRIMARY KEY (activity_id)" in apply_sql
    assert "idx_ofs_activity_state_date_resource_status" in apply_sql
    assert "ofs_event_cursor" in apply_sql
    assert "ofs_operational_sync_state" in apply_sql
    assert "DROP TABLE IF EXISTS ofs_technician_operational_state" in rollback_sql


def test_subscription_requests_exact_operational_events_and_compact_fields(monkeypatch):
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["json"] = kwargs.get("json")
        return FakeResponse(200, {"subscriptionId": "sub", "nextPage": "opaque"})

    monkeypatch.setattr(requests, "request", fake_request)
    api = OFSOperationalAPI(client=DummyClient(), settings=OperationalSettings(request_retries=1))
    sub, page = api.create_subscription()
    assert (sub, page) == ("sub", "opaque")
    configs = captured["json"]["subscriptionConfig"]
    activity_config = next(c for c in configs if "activityStarted" in c["events"])
    assert set(activity_config["events"]) == set(ACTIVITY_EVENTS)
    assert tuple(activity_config["fields"]) == ACTIVITY_FIELDS


def test_worker_source_has_no_flask_request_thread_dependency():
    root = Path(__file__).resolve().parents[1]
    source = (root / "tools/ofs_technician_operational_worker.py").read_text(encoding="utf-8")
    assert "flask" not in source.lower()
    assert "mysql_operational_lock" in source


def test_activity_status_transition_sequence_is_deterministic():
    event_types = [
        "activityTravelStarted",
        "activityStarted",
        "activityCompleted",
    ]
    expected = ["enroute", "started", "completed"]
    observed = []
    for second, event_type in enumerate(event_types, start=1):
        op = parse_activity_event({
            "eventType": event_type,
            "time": f"2026-08-26 12:00:0{second}",
            "activityDetails": {"activityId": 77, "resourceId": "T1", "date": "2026-08-26"},
        })
        observed.append(op["status"])
    assert observed == expected


def test_route_out_of_order_policy_is_independent_from_activity_order():
    newer = parse_route_event({
        "eventType": "routeActivated",
        "time": "2026-08-26 11:00:02",
        "routeDetails": {"resourceId": "T1", "date": "2026-08-26"},
        "routeChanges": {"activated": "2026-08-26 08:00"},
    })
    older = parse_route_event({
        "eventType": "routeCreated",
        "time": "2026-08-26 11:00:01",
        "routeDetails": {"resourceId": "T1", "date": "2026-08-26"},
    })
    assert not event_should_apply(newer["event_at"], newer["fingerprint"], older["event_at"], older["fingerprint"])


def test_calendar_on_call_contract_is_supported():
    row = normalize_calendar_item({
        "resourceId": "T3",
        "date": "2026-08-26",
        "on-call": {"recordType": "working", "workTimeStart": "13:00", "workTimeEnd": "18:00"},
    })
    assert row["calendar_record_type"] == "on-call"
    assert row["calendar_start_at"] == datetime(2026, 8, 26, 13, 0)


def test_expired_subscription_is_recreated_with_new_cursor_before_baseline():
    repo = FakeRepo()
    repo.cursor = {"subscription_id": "expired", "next_page": "opaque:old"}
    api = FakeAPI()
    api.subscription_exists = False
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    result = collector.ensure_subscription()
    assert result["created"] is True
    assert api.created == 1
    assert repo.cursor["next_page"] == "opaque,marker-01"
    assert repo.cursor["baseline_completed_at"] is None


def test_failed_event_page_can_be_replayed_without_cursor_corruption():
    repo = FakeRepo()
    api = FakeAPI()
    repo.cursor = {"subscription_id": "s1", "next_page": "opaque:start"}
    api.subscription_exists = True
    event_page = ([{
        "eventType": "activityStarted",
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 1, "resourceId": "T1", "date": "2026-08-26"},
    }], "opaque:next")
    api.get_events = lambda subscription_id, page: event_page
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()

    repo.fail_event_commit = True
    with pytest.raises(RuntimeError):
        collector.poll_events_once()
    assert repo.cursor["next_page"] == "opaque:start"

    repo.fail_event_commit = False
    result = collector.poll_events_once()
    assert result["page_changed"] is True
    assert repo.cursor["next_page"] == "opaque:next"


def test_reconciliation_replaces_drift_with_authoritative_activity_snapshot():
    repo = FakeRepo()
    repo.activity_rows = [{"activityId": 999, "status": "started"}]
    api = FakeAPI()
    collector = TechnicianOperationalCollector(api=api, repository=repo, root_resource_id="02")
    collector.refresh_technicians()
    collector.reconcile_activities(date(2026, 8, 26))
    assert [row["activityId"] for row in repo.activity_rows] == [1]
    assert repo.activity_rows[0]["status"] == "pending"


def test_baseline_keeps_technician_without_activity_and_supports_next_day_rebaseline():
    repo = FakeRepo()
    api = FakeAPI()
    collector = TechnicianOperationalCollector(
        api=api,
        repository=repo,
        settings=OperationalSettings(route_workers=2),
        root_resource_id="02",
    )
    first = date(2026, 8, 26)
    second = date(2026, 8, 27)
    collector.run_baseline(first)
    collector.run_baseline(second)
    assert repo.rows_ensured[0] == (first, ("T1", "T2"))
    assert repo.rows_ensured[1] == (second, ("T1", "T2"))
    assert any(row["resource_id"] == "T2" for row in repo.route_rows)
    assert repo.purges[-1] == (second, 7)


def test_request_retries_500_then_succeeds(monkeypatch):
    responses = [FakeResponse(500, {"detail": "temporary"}), FakeResponse(200, {"items": []})]
    calls = 0

    def fake_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        return responses.pop(0)

    monkeypatch.setattr(requests, "request", fake_request)
    api = OFSOperationalAPI(
        client=DummyClient(),
        settings=OperationalSettings(request_retries=2, backoff_base_seconds=0),
        sleep=lambda _: None,
    )
    assert api._request("GET", "activities") == {"items": []}
    assert calls == 2


def test_events_requires_explicit_next_page(monkeypatch):
    monkeypatch.setattr(requests, "request", lambda *args, **kwargs: FakeResponse(200, {"items": []}))
    api = OFSOperationalAPI(client=DummyClient(), settings=OperationalSettings(request_retries=1))
    with pytest.raises(OperationalAPIError, match="nextPage ausente"):
        api.get_events("s1", "opaque:input")


def test_settings_bound_route_concurrency_and_retries(monkeypatch):
    monkeypatch.setenv("OFS_OPERATIONAL_ROUTE_WORKERS", "9999")
    monkeypatch.setenv("OFS_OPERATIONAL_REQUEST_RETRIES", "9999")
    settings = OperationalSettings.from_env()
    assert settings.route_workers == 16
    assert settings.request_retries == 8


def test_runtime_source_contains_technician_and_activity_upsert_and_safe_retention():
    root = Path(__file__).resolve().parents[1]
    source = (root / "services/ofs_technician_operational_service.py").read_text(encoding="utf-8")
    assert "INSERT INTO ofs_technician_operational_state" in source
    assert "INSERT INTO ofs_activity_operational_state" in source
    assert source.count("ON DUPLICATE KEY UPDATE") >= 6
    assert "work_date < %s OR work_date > %s" in source


def test_schema_tool_uses_project_connection_and_protects_rollback():
    root = Path(__file__).resolve().parents[1]
    source = (root / "tools/ofs_technician_operational_schema.py").read_text(encoding="utf-8")
    assert "from database.connection import get_connection" in source
    assert "D06_DROP_OPERATIONAL_STATE" in source
    assert "--confirm" in source
    assert "password=" not in source.lower()


class _RouteEventCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return None


def test_route_event_for_non_technician_is_ignored_without_write():
    repo = MySQLOperationalRepository(connection_factory=lambda: None)
    cur = _RouteEventCursor()
    op = parse_route_event({
        "eventType": "routeActivated",
        "time": "2026-08-26 08:02:00",
        "routeDetails": {"resourceId": "BK_01", "date": "2026-08-26"},
        "routeChanges": {"activated": "2026-08-26 08:02"},
    })
    assert op is not None
    assert repo._apply_route_event_cur(cur, op, datetime(2026, 8, 26, 8, 3), {"T1", "T2"}) is False
    assert cur.executed == []


def test_route_event_for_technician_is_written():
    repo = MySQLOperationalRepository(connection_factory=lambda: None)
    cur = _RouteEventCursor()
    op = parse_route_event({
        "eventType": "routeActivated",
        "time": "2026-08-26 08:02:00",
        "routeDetails": {"resourceId": "T1", "date": "2026-08-26"},
        "routeChanges": {"activated": "2026-08-26 08:02"},
    })
    assert op is not None
    assert repo._apply_route_event_cur(cur, op, datetime(2026, 8, 26, 8, 3), {"T1", "T2"}) is True
    assert len(cur.executed) == 2


class _ActivityWindowCursor:
    def __init__(self, current=None):
        self.current = current
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        if sql.strip().startswith("DELETE FROM ofs_activity_operational_state"):
            self.current = None

    def fetchone(self):
        return self.current


def test_activity_moved_from_today_to_future_removes_current_row_without_creating_future_state():
    repo = MySQLOperationalRepository(connection_factory=lambda: None)
    cur = _ActivityWindowCursor({
        "activity_id": "9",
        "work_date": date(2026, 8, 26),
        "resource_id": "T1",
        "status": "pending",
        "last_event_at": datetime(2026, 8, 26, 11, 0),
        "last_event_fingerprint": "older",
    })
    op = parse_activity_event({
        "eventType": "activityMoved",
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 9, "resourceId": "T1", "date": "2026-08-26", "status": "pending"},
        "activityChanges": {"resourceId": "T2", "date": "2026-08-27"},
    })
    result = repo._apply_activity_event_cur(
        cur, op, datetime(2026, 8, 26, 12, 1), {"T1", "T2"},
        today=date(2026, 8, 26), retention_days=7,
    )
    assert result["applied"] is True
    assert result["affected"] == {(date(2026, 8, 26), "T1")}
    sql = [item[0] for item in cur.executed]
    assert any("DELETE FROM ofs_activity_operational_state" in q for q in sql)
    assert not any("INSERT INTO ofs_activity_operational_state" in q for q in sql)
    assert not any("INSERT INTO ofs_technician_operational_state" in q for q in sql)


def test_future_activity_event_without_current_row_is_ignored_without_insert():
    repo = MySQLOperationalRepository(connection_factory=lambda: None)
    cur = _ActivityWindowCursor(None)
    op = parse_activity_event({
        "eventType": "activityStarted",
        "time": "2026-08-26 12:00:00",
        "activityDetails": {"activityId": 10, "resourceId": "T1", "date": "2026-08-27", "status": "started"},
    })
    result = repo._apply_activity_event_cur(
        cur, op, datetime(2026, 8, 26, 12, 1), {"T1"},
        today=date(2026, 8, 26), retention_days=7,
    )
    assert result["applied"] is False
    sql = [item[0] for item in cur.executed]
    assert not any("INSERT INTO ofs_activity_operational_state" in q for q in sql)
    assert not any("INSERT INTO ofs_technician_operational_state" in q for q in sql)


def test_baseline_purges_after_events_to_leave_strict_retention_window():
    source = (Path(__file__).resolve().parents[1] / "services/ofs_technician_operational_service.py").read_text(encoding="utf-8")
    baseline = source[source.index("    def run_baseline"):source.index("    def poll_events_once")]
    assert baseline.index("events = self.drain_events") < baseline.index("retention = self.repository.purge_retention")
