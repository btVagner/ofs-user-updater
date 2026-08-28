from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from services.ofs_technician_alert_service import (
    ALERT_ACTIVITY_OPEN_AFTER_SHIFT,
    INTEGRITY_OK,
    SEVERITY_UNKNOWN,
    AlertRuleSettings,
    TechnicianAlertClassifier,
)
from services.ofs_technician_operational_service import (
    ACTIVITY_STATUSES,
    MySQLOperationalRepository,
    parse_activity_event,
)


WORK_DATE = date(2026, 8, 26)
NOW = datetime(2026, 8, 26, 20, 0)
ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / "static" / "js" / "dashboard_technicians.js"
WORKER = ROOT / "tools" / "ofs_technician_operational_worker.py"


class CustomerHomeCountCursor:
    """Simula somente o SELECT agregado + UPDATE em lote usados pela D11."""

    def __init__(self, activities, activity_map):
        self.activities = list(activities)
        self.activity_map = dict(activity_map)
        self.rows = []
        self.updated = {}
        self.select_sql = ""
        self.select_params = ()

    def execute(self, sql, params):
        normalized = " ".join(sql.split())
        if "FROM ofs_activity_operational_state a" not in normalized:
            raise AssertionError(f"SQL inesperado no fake D11: {normalized}")

        self.select_sql = normalized
        self.select_params = tuple(params)
        assert "INNER JOIN ofs_activity_type_map atm" in normalized
        assert "atm.code COLLATE utf8mb4_unicode_ci = a.activity_type" in normalized
        assert "atm.category = %s" in normalized
        assert "atm.is_active = 1" in normalized
        assert "include_in_bi" not in normalized

        category, work_date, *resource_ids = params
        resource_ids = set(resource_ids)
        eligible_codes = {
            code
            for code, config in self.activity_map.items()
            if str(config.get("category") or "").lower() == str(category).lower()
            and int(config.get("is_active") or 0) == 1
        }

        grouped = Counter()
        for activity in self.activities:
            if activity.get("work_date") != work_date:
                continue
            if activity.get("resource_id") not in resource_ids:
                continue
            if activity.get("activity_type") not in eligible_codes:
                continue
            grouped[(activity["resource_id"], str(activity.get("status") or "unknown").lower())] += 1

        self.rows = [
            {"resource_id": resource_id, "status": status, "total": total}
            for (resource_id, status), total in sorted(grouped.items())
        ]

    def fetchall(self):
        return list(self.rows)

    def executemany(self, sql, rows):
        normalized = " ".join(sql.split())
        assert "UPDATE ofs_technician_operational_state" in normalized
        for values in rows:
            counts = dict(zip(ACTIVITY_STATUSES, values[: len(ACTIVITY_STATUSES)]))
            counts["open_activity_count"] = values[7]
            self.updated[values[-1]] = counts


def activity(resource_id, activity_type, status="pending"):
    return {
        "resource_id": resource_id,
        "work_date": WORK_DATE,
        "activity_type": activity_type,
        "status": status,
    }


def recompute(activities, activity_map, resource_ids=("T1",)):
    cursor = CustomerHomeCountCursor(activities, activity_map)
    repository = MySQLOperationalRepository(connection_factory=lambda: None)
    result = repository._recompute_counts_cur(cursor, WORK_DATE, resource_ids, NOW)
    return cursor.updated, result, cursor


def test_customer_home_active_counts_normally():
    updated, result, _ = recompute(
        [activity("T1", "SUP", "started"), activity("T1", "INS", "pending")],
        {
            "SUP": {"category": "customer_home", "is_active": 1, "include_in_bi": 0},
            "INS": {"category": "customer_home", "is_active": 1, "include_in_bi": 0},
        },
    )
    assert updated["T1"]["started"] == 1
    assert updated["T1"]["pending"] == 1
    assert updated["T1"]["open_activity_count"] == 2
    assert result["eligible_activities"] == 2


def test_internal_alm_pending_started_suspended_never_counts():
    updated, result, _ = recompute(
        [
            activity("T1", "ALM", "pending"),
            activity("T1", "ALM", "started"),
            activity("T1", "ALM", "suspended"),
        ],
        {"ALM": {"category": "internal", "is_active": 1, "include_in_bi": 1}},
    )
    assert all(updated["T1"][status] == 0 for status in ACTIVITY_STATUSES)
    assert updated["T1"]["open_activity_count"] == 0
    assert result["eligible_activities"] == 0


def test_lunch_and_reuniao_do_not_count():
    updated, _, _ = recompute(
        [activity("T1", "LUNCH", "started"), activity("T1", "REUNIAO", "pending")],
        {
            "LUNCH": {"category": "internal", "is_active": 1},
            "REUNIAO": {"category": "internal", "is_active": 1},
        },
    )
    assert updated["T1"]["open_activity_count"] == 0


def test_activity_b2b_internal_is_excluded_even_include_in_bi_one():
    updated, _, cursor = recompute(
        [activity("T1", "ACTIVITY_B2B", "started")],
        {"ACTIVITY_B2B": {"category": "internal", "is_active": 1, "include_in_bi": 1}},
    )
    assert updated["T1"]["started"] == 0
    assert "include_in_bi" not in cursor.select_sql


def test_redes_activity_does_not_count_in_customer_home_monitor():
    updated, _, _ = recompute(
        [activity("T1", "INF_COR", "started")],
        {"INF_COR": {"category": "redes", "is_active": 1}},
    )
    assert updated["T1"]["started"] == 0
    assert updated["T1"]["open_activity_count"] == 0


def test_unmapped_activity_type_does_not_count():
    updated, _, _ = recompute([activity("T1", "NOVO_TIPO", "pending")], {})
    assert updated["T1"]["pending"] == 0
    assert updated["T1"]["open_activity_count"] == 0


def test_inactive_customer_home_does_not_count():
    updated, _, _ = recompute(
        [activity("T1", "SUP", "started")],
        {"SUP": {"category": "customer_home", "is_active": 0}},
    )
    assert updated["T1"]["started"] == 0


def test_mixed_customer_home_and_internal_counts_only_customer_home():
    updated, _, _ = recompute(
        [
            activity("T1", "SUP", "started"),
            activity("T1", "ALM", "started"),
            activity("T1", "LUNCH", "pending"),
            activity("T1", "INS", "completed"),
        ],
        {
            "SUP": {"category": "customer_home", "is_active": 1},
            "INS": {"category": "customer_home", "is_active": 1},
            "ALM": {"category": "internal", "is_active": 1},
            "LUNCH": {"category": "internal", "is_active": 1},
        },
    )
    assert updated["T1"]["started"] == 1
    assert updated["T1"]["completed"] == 1
    assert updated["T1"]["pending"] == 0
    assert updated["T1"]["open_activity_count"] == 1


def test_internal_event_keeps_activity_type_but_does_not_increment_after_recompute():
    op = parse_activity_event(
        {
            "eventType": "activityStarted",
            "time": "2026-08-26 16:00:00",
            "activityDetails": {
                "activityId": 99,
                "resourceId": "T1",
                "date": WORK_DATE.isoformat(),
                "activityType": "ALM",
            },
        }
    )
    assert op["activity_type"] == "ALM"  # preservado no read model
    updated, _, _ = recompute(
        [activity("T1", op["activity_type"], op["status"])],
        {"ALM": {"category": "internal", "is_active": 1}},
    )
    assert updated["T1"]["started"] == 0
    assert updated["T1"]["open_activity_count"] == 0


def healthy(now):
    naive = now.astimezone(timezone.utc).replace(tzinfo=None)
    return {
        "events": {"status": "ok", "last_success_at": naive - timedelta(seconds=30)},
        "activities": {"status": "ok", "last_success_at": naive - timedelta(minutes=5)},
        "calendars": {"status": "ok", "last_success_at": naive - timedelta(minutes=20)},
        "routes": {"status": "ok", "last_success_at": naive - timedelta(hours=10)},
    }


def classify_after_shift(counts):
    local_now = datetime(2026, 8, 26, 18, 0, tzinfo=timezone(timedelta(hours=-3)))
    settings = AlertRuleSettings(
        fallback_shift_start=time(8, 0),
        fallback_shift_end=time(19, 0),
        activation_tolerance_minutes=15,
        post_shift_tolerance_minutes=15,
        events_stale_seconds=180,
        activities_stale_seconds=1800,
        calendars_stale_seconds=5400,
        timezone_fallback="America/Sao_Paulo",
    )
    row = {
        "work_date": WORK_DATE,
        "resource_id": "T1",
        "resource_timezone_iana": "America/Sao_Paulo",
        "calendar_record_type": "working",
        "calendar_start_at": datetime(2026, 8, 26, 8, 0),
        "calendar_end_at": datetime(2026, 8, 26, 17, 0),
        "route_state": "ended",
        "route_started_at": datetime(2026, 8, 26, 8, 2),
        "route_ended_at": datetime(2026, 8, 26, 17, 0),
        "started_count": counts.get("started", 0),
        "suspended_count": counts.get("suspended", 0),
        "open_activity_count": counts.get("open_activity_count", 0),
    }
    return TechnicianAlertClassifier(settings).classify_one(row, healthy(local_now), now=local_now)


def test_post_shift_only_internal_activity_does_not_raise_activity_open_alert():
    updated, _, _ = recompute(
        [activity("T1", "ALM", "started"), activity("T1", "LUNCH", "pending")],
        {
            "ALM": {"category": "internal", "is_active": 1},
            "LUNCH": {"category": "internal", "is_active": 1},
        },
    )
    result = classify_after_shift(updated["T1"])
    assert ALERT_ACTIVITY_OPEN_AFTER_SHIFT not in result["alert_codes"]


def test_post_shift_customer_home_started_still_raises_activity_open_alert():
    updated, _, _ = recompute(
        [activity("T1", "SUP", "started")],
        {"SUP": {"category": "customer_home", "is_active": 1}},
    )
    result = classify_after_shift(updated["T1"])
    assert ALERT_ACTIVITY_OPEN_AFTER_SHIFT in result["alert_codes"]


def test_ui_d12_keeps_d11_backend_states_without_repeating_global_freshness_per_technician():
    js = JS.read_text(encoding="utf-8-sig")
    assert "Sincronização automática desatualizada" in js
    technician_block = js[js.index("function appendTechnicianDetails"):js.index("function appendAggregateDetails")]
    assert "integrity_state" not in technician_block
    assert "Sem estado operacional do dia" in technician_block


def test_unknown_backend_contract_is_not_reclassified_as_normal():
    classifier = TechnicianAlertClassifier()
    local_now = datetime(2026, 8, 26, 12, 0, tzinfo=timezone(timedelta(hours=-3)))
    result = classifier.classify_one(
        {
            "work_date": WORK_DATE,
            "resource_id": "T1",
            "resource_timezone_iana": "America/Sao_Paulo",
            "calendar_record_type": None,
            "route_state": "unknown",
            "started_count": 0,
            "suspended_count": 0,
            "open_activity_count": 0,
        },
        {},
        now=local_now,
    )
    assert result["operational_severity"] == SEVERITY_UNKNOWN
    assert result["integrity_state"] != INTEGRITY_OK


def test_recompute_cli_is_local_locked_and_does_not_require_collector_first():
    source = WORKER.read_text(encoding="utf-8")
    recompute_pos = source.index("if args.recompute_counts_once:")
    collector_pos = source.index("collector = TechnicianOperationalCollector")
    assert recompute_pos < collector_pos
    assert "with mysql_operational_lock():" in source
    assert "--recompute-counts-once" in source


def test_d11_preserves_lazy_loading_and_prefix_contract():
    js = JS.read_text(encoding="utf-8-sig")
    assert 'mode: "children"' in js
    assert 'mode: "full"' not in js
    assert "mode=full" not in js
    assert "/ofs/" not in js
