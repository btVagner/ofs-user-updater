from __future__ import annotations

from datetime import date

from flask import Flask

import routes.ofs_technician_monitor_routes as monitor_routes


class FakeMonitorService:
    def build_summary(self, work_date):
        return {"work_date": work_date.isoformat(), "total_technicians": 1}, {}

    def build_tree(self, work_date, **kwargs):
        return {
            "work_date": work_date.isoformat(),
            "mode": kwargs["mode"],
            "parent_id": kwargs["parent_id"],
            "only_problems": kwargs["only_problems"],
            "filter": kwargs["filter_name"],
            "detail": kwargs["detail"],
            "nodes": [],
        }, {}


class FailingMonitorService:
    def build_summary(self, work_date):
        raise RuntimeError("SQL SELECT secret_should_not_leak")

    def build_tree(self, work_date, **kwargs):
        raise RuntimeError("SQL SELECT secret_should_not_leak")


def make_app(monkeypatch, service):
    app = Flask(__name__)
    app.secret_key = "test-secret"

    @app.route("/login", endpoint="login")
    def login():
        return "login"

    monkeypatch.setattr(monitor_routes, "_service", lambda: service)
    monitor_routes.init_app(app)
    app.testing = True
    return app


def login_session(client, *, allowed=True):
    with client.session_transaction() as session:
        session["usuario_logado"] = "tester"
        session["usuario_id"] = 1
        session["tipo_id"] = 2
        session["permissoes"] = [monitor_routes.PERMISSION] if allowed else []


def test_summary_authorized_user_gets_json(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/summary?date=2026-08-26")
    assert response.status_code == 200
    assert response.get_json() == {
        "ok": True,
        "data": {"work_date": "2026-08-26", "total_technicians": 1},
    }


def test_tree_defaults_to_incremental_root_contract(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/tree?date=2026-08-26")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["mode"] == "children"
    assert data["parent_id"] is None


def test_tree_authorized_user_supports_incremental_contract(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get(
        "/dashboard/technicians/tree?date=2026-08-26&parent_id=BK1&only_problems=1&detail=1"
    )
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["mode"] == "children"
    assert data["parent_id"] == "BK1"
    assert data["only_problems"] is True
    assert data["filter"] is None
    assert data["detail"] is True


def test_unauthenticated_preserves_existing_login_required_behavior(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    response = client.get("/dashboard/technicians/summary?date=2026-08-26")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")


def test_authenticated_without_dashboard_permission_gets_403_json(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=False)
    response = client.get("/dashboard/technicians/summary?date=2026-08-26")
    assert response.status_code == 403
    assert response.get_json()["error"]["code"] == "FORBIDDEN"


def test_invalid_date_is_400_without_touching_service(monkeypatch):
    class MustNotRun:
        def build_summary(self, work_date):
            raise AssertionError("service should not run")

    client = make_app(monkeypatch, MustNotRun()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/summary?date=26-08-2026")
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "INVALID_DATE"


def test_invalid_tree_mode_is_400(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/tree?date=2026-08-26&mode=other")
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "INVALID_TREE_MODE"


def test_internal_read_error_does_not_leak_sql_or_exception(monkeypatch):
    app = make_app(monkeypatch, FailingMonitorService())
    app.testing = False
    client = app.test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/summary?date=2026-08-26")
    assert response.status_code == 500
    body = response.get_data(as_text=True)
    assert "secret_should_not_leak" not in body
    assert "SELECT" not in body
    assert response.get_json()["error"]["code"] == "READ_MODEL_ERROR"


def test_tree_accepts_d12_server_filter(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/tree?date=2026-08-26&filter=active_route")
    assert response.status_code == 200
    assert response.get_json()["data"]["filter"] == "active_route"


def test_invalid_tree_filter_is_400(monkeypatch):
    client = make_app(monkeypatch, FakeMonitorService()).test_client()
    login_session(client, allowed=True)
    response = client.get("/dashboard/technicians/tree?date=2026-08-26&filter=not-valid")
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "INVALID_TREE_FILTER"
