from flask import Flask

import routes.ofs_operational_monitor_routes as monitor_routes
from services.ofs_operational_monitor_service import MonitorRefreshCooldown, MonitorRefreshInProgress


SNAPSHOT = {"scope_key": "casa-cliente", "status": "ready", "has_payload": True, "payload": {"technicians_count": 3}, "remaining_seconds": 480}


class FakeService:
    def __init__(self):
        self.refresh_calls = []

    def get_snapshot(self, scope):
        assert scope == "casa-cliente"
        return dict(SNAPSHOT)

    def refresh(self, scope, *, actor_id, actor_username):
        self.refresh_calls.append((scope, actor_id, actor_username))
        return dict(SNAPSHOT)


def make_app(monkeypatch, service):
    app = Flask(__name__)
    app.secret_key = "test-secret"

    @app.route("/login", endpoint="login")
    def login():
        return "login"

    @app.route("/", endpoint="home")
    def home():
        return "home"

    monkeypatch.setattr(monitor_routes, "_service", lambda: service)
    monitor_routes.init_app(app)
    app.testing = True
    return app


def login(client, allowed=True):
    with client.session_transaction() as session:
        session["usuario_logado"] = "tester"
        session["usuario_id"] = 7
        session["tipo_id"] = 2
        session["permissoes"] = [monitor_routes.PERMISSION] if allowed else []


def test_data_requires_dedicated_permission(monkeypatch):
    client = make_app(monkeypatch, FakeService()).test_client()
    login(client, allowed=False)
    response = client.get("/ofs/monitor-operacional/data")
    assert response.status_code == 403
    assert response.get_json()["error"]["code"] == "FORBIDDEN"


def test_data_reads_shared_snapshot_without_refresh(monkeypatch):
    service = FakeService()
    client = make_app(monkeypatch, service).test_client()
    login(client)
    response = client.get("/ofs/monitor-operacional/data?scope=casa-cliente")
    assert response.status_code == 200
    assert response.get_json()["data"]["payload"]["technicians_count"] == 3
    assert service.refresh_calls == []


def test_refresh_records_authenticated_actor(monkeypatch):
    service = FakeService()
    client = make_app(monkeypatch, service).test_client()
    login(client)
    response = client.post("/ofs/monitor-operacional/refresh", json={"scope": "casa-cliente"})
    assert response.status_code == 200
    assert service.refresh_calls == [("casa-cliente", 7, "tester")]


def test_refresh_cooldown_returns_current_snapshot(monkeypatch):
    class CooldownService(FakeService):
        def refresh(self, *args, **kwargs):
            raise MonitorRefreshCooldown(dict(SNAPSHOT))

    client = make_app(monkeypatch, CooldownService()).test_client()
    login(client)
    response = client.post("/ofs/monitor-operacional/refresh", json={"scope": "casa-cliente"})
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "REFRESH_COOLDOWN"


def test_refresh_lock_returns_423(monkeypatch):
    class LockedService(FakeService):
        def refresh(self, *args, **kwargs):
            raise MonitorRefreshInProgress("busy")

    client = make_app(monkeypatch, LockedService()).test_client()
    login(client)
    response = client.post("/ofs/monitor-operacional/refresh", json={"scope": "casa-cliente"})
    assert response.status_code == 423
    assert response.get_json()["error"]["code"] == "REFRESH_IN_PROGRESS"

