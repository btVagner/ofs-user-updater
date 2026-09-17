from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from services.ofs_technician_operational_service import MySQLOperationalRepository
from tools.repair_ofs_route_timestamps import parse_args, repair_route_timestamps


WORK_DATE = date(2026, 9, 17)


def row(resource_id="T1", *, started=None, reactivated=None, ended=None, work_date=WORK_DATE, event_type=None):
    return {
        "resource_id": resource_id,
        "work_date": work_date,
        "route_started_at": started,
        "route_reactivated_at": reactivated,
        "route_ended_at": ended,
        "route_last_event_type": event_type,
    }


class FakeRepository:
    def __init__(self, rows):
        self.rows = list(rows)
        self.updates = []

    def get_route_timestamp_rows(self, work_date):
        return [item for item in self.rows if item["work_date"] == work_date]

    def update_route_timestamps(self, work_date, resource_id, values, *, now=None):
        self.updates.append((work_date, resource_id, dict(values)))
        return 1


class FakeAPI:
    def __init__(self, payloads=None, errors=None):
        self.payloads = payloads or {}
        self.errors = errors or {}
        self.calls = []

    def get_route(self, resource_id, work_date):
        self.calls.append((resource_id, work_date))
        if resource_id in self.errors:
            raise self.errors[resource_id]
        return dict(self.payloads.get(resource_id, {})), 1


def run(tmp_path, rows, payloads, *, apply=False, errors=None):
    repository = FakeRepository(rows)
    report = repair_route_timestamps(
        WORK_DATE,
        repository=repository,
        api=FakeAPI(payloads, errors),
        apply=apply,
        evidence_path=tmp_path / "evidence.json",
    )
    return repository, report


def test_correct_database_produces_no_change(tmp_path):
    repository, report = run(
        tmp_path,
        [row(started=datetime(2026, 9, 17, 8, 0))],
        {"T1": {"routeStartTime": "2026-09-17T08:00:00-03:00"}},
    )
    assert report["divergent_fields"] == 0
    assert repository.updates == []


def test_dry_run_detects_utc_shift_without_writing(tmp_path):
    repository, report = run(
        tmp_path,
        [row(started=datetime(2026, 9, 17, 11, 0))],
        {"T1": {"routeStartTime": "2026-09-17T08:00:00-03:00"}},
    )
    assert report["divergent_fields"] == 1
    assert report["sample"][0]["valor_anterior"] == datetime(2026, 9, 17, 11, 0)
    assert report["sample"][0]["valor_novo"] == datetime(2026, 9, 17, 8, 0)
    assert repository.updates == []


def test_apply_updates_only_divergent_field(tmp_path):
    repository, report = run(
        tmp_path,
        [row(started=datetime(2026, 9, 17, 11, 0), ended=datetime(2026, 9, 17, 18, 2))],
        {"T1": {"routeStartTime": "2026-09-17T08:00:00-03:00", "routeEndTime": "2026-09-17T18:02:00-03:00"}},
        apply=True,
    )
    assert repository.updates == [(WORK_DATE, "T1", {"route_started_at": datetime(2026, 9, 17, 8, 0)})]
    assert report["corrected_fields"] == 1
    assert report["changes"][0]["applied"] is True


def test_later_route_updated_does_not_hide_wrong_start(tmp_path):
    repository, report = run(
        tmp_path,
        [row(started=datetime(2026, 9, 17, 11, 0), event_type="routeUpdated")],
        {"T1": {"routeStartTime": "2026-09-17T08:00:00-03:00"}},
    )
    assert report["divergent_fields"] == 1
    assert repository.updates == []


def test_reactivation_divergence_is_detected(tmp_path):
    _, report = run(
        tmp_path,
        [row(reactivated=datetime(2026, 9, 17, 12, 15))],
        {"T1": {"routeReactivationTime": "2026-09-17T09:15:00-03:00"}},
    )
    assert report["changes"][0]["campo"] == "route_reactivated_at"


def test_end_divergence_is_detected(tmp_path):
    _, report = run(
        tmp_path,
        [row(ended=datetime(2026, 9, 17, 21, 2))],
        {"T1": {"routeEndTime": "2026-09-17T18:02:00-03:00"}},
    )
    assert report["changes"][0]["campo"] == "route_ended_at"


def test_other_work_date_is_not_read_or_updated(tmp_path):
    repository, report = run(
        tmp_path,
        [
            row(started=datetime(2026, 9, 17, 11, 0)),
            row("T2", started=datetime(2026, 9, 16, 11, 0), work_date=date(2026, 9, 16)),
        ],
        {
            "T1": {"routeStartTime": "2026-09-17T08:00:00-03:00"},
            "T2": {"routeStartTime": "2026-09-16T08:00:00-03:00"},
        },
        apply=True,
    )
    assert report["analyzed_resources"] == 1
    assert [item[1] for item in repository.updates] == ["T1"]


def test_ofs_failure_for_one_resource_never_updates_that_resource(tmp_path):
    repository, report = run(
        tmp_path,
        [row("T1", started=datetime(2026, 9, 17, 11, 0)), row("T2", started=datetime(2026, 9, 17, 11, 0))],
        {"T2": {"routeStartTime": "2026-09-17T08:00:00-03:00"}},
        apply=True,
        errors={"T1": RuntimeError("timeout")},
    )
    assert report["errors_count"] == 1
    assert [item[1] for item in repository.updates] == ["T2"]


def test_report_and_evidence_redact_credentials(tmp_path):
    secret = "super-secret-value"
    _, report = run(
        tmp_path,
        [row()],
        {},
        errors={"T1": RuntimeError(f"Authorization={secret}")},
    )
    rendered = json.dumps(report, default=str)
    evidence = (tmp_path / "evidence.json").read_text(encoding="utf-8")
    assert secret not in rendered
    assert secret not in evidence
    assert "<redacted>" in rendered


def test_dry_run_never_calls_update_even_with_multiple_divergences(tmp_path):
    repository, report = run(
        tmp_path,
        [row(started=datetime(2026, 9, 17, 11, 0), ended=datetime(2026, 9, 17, 21, 2))],
        {"T1": {"routeStartTime": "2026-09-17T08:00:00-03:00", "routeEndTime": "2026-09-17T18:02:00-03:00"}},
    )
    assert report["divergent_fields"] == 2
    assert repository.updates == []


def test_cli_is_dry_run_by_default_and_requires_explicit_apply():
    dry_run = parse_args(["--date", "2026-09-17"])
    apply = parse_args(["--date", "2026-09-17", "--apply"])
    assert dry_run.apply is False
    assert apply.apply is True


def test_mysql_update_is_allowlisted_and_scoped_to_exact_row():
    class Cursor:
        rowcount = 1

        def __init__(self):
            self.executed = None

        def execute(self, sql, params):
            self.executed = (" ".join(sql.split()), params)

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.committed = False

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.committed = True

        def rollback(self):
            pass

        def close(self):
            pass

    connection = Connection()
    repository = MySQLOperationalRepository(connection_factory=lambda: connection)
    repaired = datetime(2026, 9, 17, 8, 0)
    repository.update_route_timestamps(WORK_DATE, "T1", {"route_started_at": repaired})
    sql, params = connection.cursor_instance.executed
    assert sql == (
        "UPDATE ofs_technician_operational_state "
        "SET route_started_at=%s,updated_at=%s WHERE work_date=%s AND resource_id=%s"
    )
    assert params[0] == repaired
    assert params[-2:] == (WORK_DATE, "T1")
    assert connection.committed is True
    with pytest.raises(ValueError, match="nao permitidos"):
        repository.update_route_timestamps(WORK_DATE, "T1", {"route_state": "ended"})
