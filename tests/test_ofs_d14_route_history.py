from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import inspect

from flask import Flask

import routes.ofs_technician_monitor_routes as monitor_routes
from services.ofs_technician_alert_service import AlertRuleSettings, TechnicianAlertClassifier
from services.ofs_technician_monitor_service import (
    ROUTE_HISTORY_RETENTION_DAYS,
    TechnicianMonitorService,
)


ROOT = Path(__file__).resolve().parents[1]
WORK_DATE = date(2026, 8, 31)


class HistoryRepository:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    def load_route_history(self, resource_id, *, end_date, retention_days, root_resource_id):
        self.calls.append({
            "resource_id": resource_id,
            "end_date": end_date,
            "retention_days": retention_days,
            "root_resource_id": root_resource_id,
        })
        return list(self.rows), 1.25


def history_service(rows):
    settings = AlertRuleSettings(timezone_fallback="America/Sao_Paulo")
    repository = HistoryRepository(rows)
    service = TechnicianMonitorService(
        repository=repository,
        classifier=TechnicianAlertClassifier(settings),
    )
    return service, repository


def history_row(work_date, *, started=None, reactivated=None, ended=None, tz="America/Sao_Paulo"):
    return {
        "work_date": work_date,
        "resource_id": "T1",
        "route_started_at": started,
        "route_reactivated_at": reactivated,
        "route_ended_at": ended,
        "resource_timezone": None,
        "resource_timezone_iana": tz,
    }


def test_route_history_activation_without_end():
    service, _ = history_service([
        history_row(WORK_DATE, started=datetime(2026, 8, 31, 8, 3)),
    ])
    payload, metrics = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    day = payload["days"][0]
    assert day["date_label"] == "31/08/2026"
    assert day["activation_time"] == "08:03"
    assert day["end_time"] is None
    assert metrics["mysql_queries_total"] == 1


def test_route_history_activation_and_end():
    service, _ = history_service([
        history_row(
            WORK_DATE,
            started=datetime(2026, 8, 31, 8, 1),
            ended=datetime(2026, 8, 31, 17, 12),
        ),
    ])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    day = payload["days"][0]
    assert day["activation_time"] == "08:01"
    assert day["end_time"] == "17:12"


def test_route_history_without_start_or_end():
    service, _ = history_service([history_row(WORK_DATE)])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    day = payload["days"][0]
    assert day["activation_time"] is None
    assert day["end_time"] is None


def test_route_history_end_without_start_does_not_invent_activation():
    service, _ = history_service([
        history_row(
            WORK_DATE,
            reactivated=datetime(2026, 8, 31, 12, 30),
            ended=datetime(2026, 8, 31, 17, 4),
        ),
    ])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    day = payload["days"][0]
    assert day["activation_time"] is None
    assert day["reactivation_time"] == "12:30"
    assert day["end_time"] == "17:04"


def test_route_history_multiple_days_are_sorted_newest_first():
    service, _ = history_service([
        history_row(date(2026, 8, 29), started=datetime(2026, 8, 29, 8, 5)),
        history_row(date(2026, 8, 31), started=datetime(2026, 8, 31, 8, 3)),
        history_row(date(2026, 8, 30), started=datetime(2026, 8, 30, 8, 1)),
    ])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    assert [item["work_date"] for item in payload["days"]] == [
        "2026-08-31",
        "2026-08-30",
        "2026-08-29",
    ]


def test_route_history_uses_existing_seven_day_window_only():
    service, repository = history_service([])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    assert repository.calls == [{
        "resource_id": "T1",
        "end_date": WORK_DATE,
        "retention_days": ROUTE_HISTORY_RETENTION_DAYS,
        "root_resource_id": "02",
    }]
    assert payload["window_start"] == "2026-08-25"
    assert payload["window_end"] == "2026-08-31"
    assert payload["retention_days"] == 7


def test_route_history_formats_timezone_aware_values_for_sao_paulo_and_keeps_naive_wall_clock():
    service, _ = history_service([
        history_row(
            WORK_DATE,
            started=datetime(2026, 8, 31, 11, 3, tzinfo=timezone.utc),
            ended=datetime(2026, 8, 31, 17, 12),
        ),
    ])
    payload, _ = service.build_route_history("T1", end_date=WORK_DATE, root_resource_id="02")
    day = payload["days"][0]
    assert day["activation_time"] == "08:03"
    assert day["end_time"] == "17:12"
    assert day["timezone"] == "America/Sao_Paulo"
    assert payload["display_timezone"] == "America/Sao_Paulo"


def test_route_history_query_contract_is_mysql_only_and_index_friendly():
    source = inspect.getsource(__import__("services.ofs_technician_monitor_service", fromlist=["dummy"]))
    assert "FROM ofs_technician_operational_state s" in source
    assert "s.resource_id = %s" in source
    assert "s.work_date BETWEEN %s AND %s" in source
    assert "ORDER BY s.work_date DESC" in source
    assert "OFSClient" not in source
    assert "requests.get" not in source
    assert "requests.post" not in source


class FakeHistoryService:
    def build_route_history(self, resource_id):
        return {
            "resource_id": resource_id,
            "window_start": "2026-08-25",
            "window_end": "2026-08-31",
            "retention_days": 7,
            "data_available": True,
            "display_timezone": "America/Sao_Paulo",
            "days": [{
                "work_date": "2026-08-31",
                "date_label": "31/08/2026",
                "activation_time": "08:03",
                "reactivation_time": None,
                "end_time": None,
                "timezone": "America/Sao_Paulo",
            }],
        }, {"mysql_queries_total": 1}

    def build_summary(self, work_date):
        return {}, {}

    def build_tree(self, work_date, **kwargs):
        return {"nodes": []}, {}


def make_app(monkeypatch):
    app = Flask(__name__)
    app.secret_key = "test-secret"

    @app.route("/login", endpoint="login")
    def login():
        return "login"

    monkeypatch.setattr(monitor_routes, "_service", lambda: FakeHistoryService())
    monitor_routes.init_app(app)
    app.testing = True
    return app


def login_session(client):
    with client.session_transaction() as session:
        session["usuario_logado"] = "tester"
        session["usuario_id"] = 1
        session["tipo_id"] = 2
        session["permissoes"] = [monitor_routes.PERMISSION]


def test_route_history_endpoint_is_small_local_detail_request(monkeypatch):
    client = make_app(monkeypatch).test_client()
    login_session(client)
    response = client.get("/dashboard/technicians/T1/route-history")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["resource_id"] == "T1"
    assert data["days"][0]["activation_time"] == "08:03"
    assert data["days"][0]["end_time"] is None


def test_frontend_loads_history_only_when_technician_is_expanded():
    template = (ROOT / "templates" / "dashboard_operacional.html").read_text(encoding="utf-8")
    js = (ROOT / "static" / "js" / "dashboard_technicians.js").read_text(encoding="utf-8")
    assert "data-technicians-route-history-url-template" in template
    initial_block = js[js.index("async function loadInitial"):js.index("async function runWithConcurrency")]
    assert "loadRouteHistory" not in initial_block
    click_start = js.index('tree.addEventListener("click"')
    click_end = js.index("if (filters)", click_start)
    click_block = js[click_start:click_end]
    assert "data-technician-toggle" in click_block
    assert "await loadRouteHistory(resourceId" in click_block


def test_frontend_preserves_incremental_tree_and_prefix_contract():
    js = (ROOT / "static" / "js" / "dashboard_technicians.js").read_text(encoding="utf-8")
    assert 'mode: "children"' in js
    assert 'parent_id: parentId' in js
    assert 'mode: "full"' not in js
    assert "mode=full" not in js
    assert "/ofs/" not in js
    assert "routeHistoryUrlTemplate.replace" in js


def test_route_history_ui_uses_compact_missing_information_contract():
    js = (ROOT / "static" / "js" / "dashboard_technicians.js").read_text(encoding="utf-8")
    css = (ROOT / "static" / "css" / "dashboard_technicians.css").read_text(encoding="utf-8")
    assert 'title.textContent = "Histórico de rota"' in js
    assert '["Data", "Ativação", "Inativação"]' in js
    assert 'missing.title = "Ainda não há informação"' in js
    assert "technicians-route-history-grid" in css
    assert "technicians-route-history-row" in css
