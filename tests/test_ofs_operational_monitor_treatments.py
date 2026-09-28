import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest
from flask import Flask

import routes.ofs_operational_monitor_routes as monitor_routes
import services.ofs_operational_monitor_treatment_service as treatment_module
from services.ofs_operational_monitor_treatment_service import (
    MonitorTreatmentService,
    TreatmentError,
    TREAT_PERMISSION,
    SUPERVISE_PERMISSION,
)


class FakeTreatments:
    def __init__(self):
        self.claims = []
        self.supervision_calls = []

    def list_current(self, scope):
        return {"work_date": "2026-09-28", "items": []}

    def claim(self, scope, work_date, indicator, item_key, actor):
        self.claims.append((scope, work_date, indicator, item_key, actor["username"]))
        return {"item": {"status": "analysis"}, "token": "test-token"}

    def change(self, *args):
        return {"item": {"status": "resolved"}}

    def supervision(self, *args):
        self.supervision_calls.append(args)
        return {"ranking": [], "recent": [], "total_resolved": 0}


def _client(monkeypatch, treatment_service, permissions):
    app = Flask(__name__)
    app.secret_key = "test-secret"

    @app.route("/login", endpoint="login")
    def login():
        return "login"

    @app.route("/", endpoint="home")
    def home():
        return "home"

    monkeypatch.setattr(monitor_routes, "_treatment_service", lambda: treatment_service)
    monitor_routes.init_app(app)
    app.testing = True
    client = app.test_client()
    with client.session_transaction() as session:
        session["usuario_logado"] = "agent-1"
        session["usuario_id"] = 11
        session["permissoes"] = permissions
        session["ofs_monitor_csrf"] = "test-csrf"
    return client


def test_claim_requires_treatment_permission_and_csrf(monkeypatch):
    service = FakeTreatments()
    client = _client(monkeypatch, service, [monitor_routes.PERMISSION])
    payload = {"scope": "casa-cliente", "work_date": "2026-09-28", "indicator": "late", "item_key": "123"}
    assert client.post("/ofs/monitor-operacional/tratativas/assumir", json=payload).status_code == 403
    client = _client(monkeypatch, service, [monitor_routes.PERMISSION, TREAT_PERMISSION])
    assert client.post("/ofs/monitor-operacional/tratativas/assumir", json=payload).get_json()["error"]["code"] == "INVALID_CSRF"
    response = client.post("/ofs/monitor-operacional/tratativas/assumir", json=payload,
                           headers={"X-Monitor-CSRF": "test-csrf"})
    assert response.status_code == 200
    assert service.claims == [("casa-cliente", "2026-09-28", "late", "123", "agent-1")]


def test_supervision_requires_separate_permission(monkeypatch):
    service = FakeTreatments()
    client = _client(monkeypatch, service, [monitor_routes.PERMISSION, TREAT_PERMISSION])
    assert client.get("/ofs/monitor-operacional/supervisao/dados").status_code == 403
    client = _client(monkeypatch, service, [SUPERVISE_PERMISSION])
    assert client.get("/ofs/monitor-operacional/supervisao/dados").status_code == 200
    assert client.get("/ofs/monitor-operacional/supervisao/dados?preset=week&top=10&action=waiting&agent=agent-1").status_code == 200
    assert service.supervision_calls[-1] == ("casa-cliente", "week", 10, None, None, "waiting", "agent-1")


def test_supervision_validates_recent_filters_before_database_access():
    service = MonitorTreatmentService(connection_factory=lambda: None)
    with pytest.raises(TreatmentError) as error:
        service.supervision("casa-cliente", "week", 5, action="claim")
    assert error.value.code == "INVALID_ACTION"
    with pytest.raises(TreatmentError) as error:
        service.supervision("casa-cliente", "week", 5, agent="x" * 151)
    assert error.value.code == "INVALID_AGENT"


@pytest.mark.parametrize("preset,duration", [
    ("hour", timedelta(hours=1)),
    ("six_hours", timedelta(hours=6)),
    ("day", timedelta(days=1)),
    ("week", timedelta(days=7)),
])
def test_supervision_uses_rolling_periods(monkeypatch, preset, duration):
    fixed_now = datetime(2026, 9, 28, 14, 30)
    monkeypatch.setattr(treatment_module, "utc_now_naive", lambda: fixed_now)

    class Cursor:
        def execute(self, sql, params):
            pass

        def fetchone(self):
            return {"total": 0}

        def fetchall(self):
            return []

        def close(self):
            pass

    class Connection:
        def cursor(self, dictionary=False):
            assert dictionary
            return Cursor()

        def close(self):
            pass

    service = MonitorTreatmentService(connection_factory=Connection)
    data = service.supervision("casa-cliente", preset, 5)
    assert datetime.fromisoformat(data["since"]) == fixed_now - duration
    assert datetime.fromisoformat(data["until"]) == fixed_now


def test_supervision_summary_and_recent_filters_use_local_parameterized_queries():
    class Cursor:
        def __init__(self):
            self.queries = []
            self.sql = ""

        def execute(self, sql, params):
            self.sql = sql
            self.queries.append((sql, params))

        def fetchone(self):
            if "status='analysis'" in self.sql:
                return {"total": 2}
            if "status='waiting'" in self.sql:
                return {"total": 3}
            return {"total": 5}

        def fetchall(self):
            if "resolved_by_username AS username" in self.sql:
                return [{"username": "agent-1", "total": 5}]
            if "SELECT DISTINCT actor_username" in self.sql:
                return [{"actor_username": "agent-1"}]
            if "FROM ofs_operational_monitor_treatment_event e" in self.sql:
                return []
            return []

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def cursor(self, dictionary=False):
            assert dictionary
            return self.cursor_instance

        def close(self):
            pass

    connection = Connection()
    service = MonitorTreatmentService(connection_factory=lambda: connection)
    data = service.supervision("casa-cliente", "week", 5, action="waiting", agent="agent-1")
    assert (data["total_resolved"], data["active_total"], data["waiting_total"]) == (5, 2, 3)
    assert data["agents"] == ["agent-1"]
    recent_sql, recent_params = next((sql, params) for sql, params in connection.cursor_instance.queries
                                     if "FROM ofs_operational_monitor_treatment_event e" in sql)
    assert "AND e.action=%s" in recent_sql and "AND e.actor_username=%s" in recent_sql
    assert recent_params[-2:] == ("waiting", "agent-1")


def test_rejects_black_and_stale_snapshot_before_claim(monkeypatch):
    service = MonitorTreatmentService(connection_factory=lambda: None)
    with pytest.raises(TreatmentError) as error:
        service.claim("casa-cliente", "2026-09-28", "black", "123", {"id": 1, "username": "a"})
    assert error.value.code == "INVALID_ITEM"
    monkeypatch.setattr(service, "_snapshot_item", lambda *args: (_ for _ in ()).throw(TreatmentError("STALE_SNAPSHOT", "stale", 409)))
    with pytest.raises(TreatmentError) as error:
        service.claim("casa-cliente", "2026-09-28", "late", "123", {"id": 1, "username": "a"})
    assert error.value.code == "STALE_SNAPSHOT"


@pytest.mark.skipif(os.getenv("OFS_MONITOR_TREATMENT_DB_TEST") != "1", reason="Teste MySQL local opt-in")
def test_mysql_claim_is_atomic_and_old_token_cannot_finish(monkeypatch):
    from database.connection import get_connection

    item_key = "codex-test-" + uuid.uuid4().hex
    work_date = "2099-12-31"
    service = MonitorTreatmentService()
    monkeypatch.setattr(MonitorTreatmentService, "_snapshot_item", lambda self, *args: {
        "id": item_key, "appt": "", "tech": "Teste", "resource_id": "10", "area": "Teste", "states": ["SP"],
    })
    identity = ("casa-cliente", work_date, "late", item_key)
    agents = [{"id": 111, "username": "codex-agent-a"}, {"id": 222, "username": "codex-agent-b"}]
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda actor: _claim_result(service, identity, actor), agents))
        successes = [result for result in outcomes if "token" in result]
        failures = [result for result in outcomes if "error" in result]
        assert len(successes) == 1
        assert failures == [{"error": "ALREADY_CLAIMED"}]
        winner = agents[outcomes.index(successes[0])]
        loser = agents[1 - outcomes.index(successes[0])]
        with pytest.raises(TreatmentError) as error:
            service.change(*identity, loser, successes[0]["token"], "resolved")
        assert error.value.code == "CLAIM_LOST"
        service.change(*identity, winner, successes[0]["token"], "waiting")
        second = service.claim(*identity, loser)
        with pytest.raises(TreatmentError) as error:
            service.change(*identity, winner, successes[0]["token"], "resolved")
        assert error.value.code == "CLAIM_LOST"
        service.change(*identity, loser, second["token"], "resolved", "Concluído no teste")
        with pytest.raises(TreatmentError) as error:
            service.claim(*identity, winner)
        assert error.value.code == "ALREADY_RESOLVED"
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT status,resolved_by_username FROM ofs_operational_monitor_treatment WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            assert cur.fetchone() == ("resolved", loser["username"])
            cur.execute("SELECT action FROM ofs_operational_monitor_treatment_event WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s ORDER BY id", identity)
            assert [row[0] for row in cur.fetchall()] == ["claim", "waiting", "claim", "resolved"]
        finally:
            cur.close()
            conn.close()
    finally:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM ofs_operational_monitor_treatment_event WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            cur.execute("DELETE FROM ofs_operational_monitor_treatment WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            conn.commit()
        finally:
            cur.close()
            conn.close()


@pytest.mark.skipif(os.getenv("OFS_MONITOR_TREATMENT_DB_TEST") != "1", reason="Teste MySQL local opt-in")
def test_mysql_expired_lease_can_be_reclaimed(monkeypatch):
    from database.connection import get_connection

    item_key = "codex-test-" + uuid.uuid4().hex
    identity = ("casa-cliente", "2099-12-31", "idle", item_key)
    monkeypatch.setattr(MonitorTreatmentService, "_snapshot_item", lambda self, *args: {
        "resource_id": item_key, "tech": "Teste", "area": "Teste", "states": ["SP"],
    })
    service = MonitorTreatmentService()
    first_actor = {"id": 111, "username": "codex-agent-a"}
    second_actor = {"id": 222, "username": "codex-agent-b"}
    try:
        first = service.claim(*identity, first_actor)
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE ofs_operational_monitor_treatment SET lease_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            conn.commit()
        finally:
            cur.close()
            conn.close()
        second = service.claim(*identity, second_actor)
        assert second["token"] != first["token"]
        with pytest.raises(TreatmentError) as error:
            service.change(*identity, first_actor, first["token"], "resolved")
        assert error.value.code == "CLAIM_LOST"
        assert service.change(*identity, second_actor, second["token"], "open")["item"]["status"] == "open"
    finally:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM ofs_operational_monitor_treatment_event WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            cur.execute("DELETE FROM ofs_operational_monitor_treatment WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s", identity)
            conn.commit()
        finally:
            cur.close()
            conn.close()


def _claim_result(service, identity, actor):
    try:
        return service.claim(*identity, actor)
    except TreatmentError as exc:
        return {"error": exc.code}
