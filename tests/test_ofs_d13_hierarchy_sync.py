from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests


ROOT = Path(__file__).resolve().parents[1]

# Evita abrir o pool MySQL real ao importar o serviço isoladamente.
if "database.connection" not in sys.modules:
    fake_connection_module = types.ModuleType("database.connection")
    fake_connection_module.get_connection = lambda: (_ for _ in ()).throw(
        AssertionError("conexão real não deve ser usada neste teste")
    )
    sys.modules["database.connection"] = fake_connection_module

from services import ofs_resource_hierarchy_service as hierarchy_service  # noqa: E402
from services.ofs_technician_monitor_service import TechnicianMonitorService  # noqa: E402


class FakeOFSClient:
    base_url = "https://ofs.example/rest/ofscCore/v1"

    def __init__(self, descendants=None, fail=False):
        self.descendants = list(descendants or [])
        self.fail = fail
        self.calls = []

    def authenticated_get(self, url):
        self.calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if parsed.path.endswith("/resources/02"):
            return {
                "resourceId": "02",
                "name": "Raiz",
                "resourceType": "GR",
                "status": "active",
                "timeZone": "America/Sao_Paulo",
            }
        if parsed.path.endswith("/resources/02/descendants"):
            if self.fail:
                raise requests.Timeout("token=nao-deve-aparecer https://ofs.example/private")
            offset = int((query.get("offset") or ["0"])[0])
            limit = int((query.get("limit") or ["100"])[0])
            return {
                "totalResults": len(self.descendants),
                "items": self.descendants[offset:offset + limit],
            }
        raise AssertionError(f"URL inesperada: {url}")


class MemoryStatusRepository:
    def __init__(self):
        self.rows = {}
        self.status_updates = []
        self.replace_calls = 0

    def replace_snapshot(self, rows, root_resource_id, seen_at):
        self.replace_calls += 1
        incoming = {row["resource_id"]: dict(row) for row in rows}
        removed = len(set(self.rows) - set(incoming))
        self.rows = incoming
        return removed

    def update_sync_status(self, **kwargs):
        self.status_updates.append(dict(kwargs))


def resource(resource_id: str, parent: str | None, *, status="active", kind="TCV"):
    return {
        "resourceId": resource_id,
        "parentResourceId": parent,
        "name": resource_id,
        "resourceType": kind,
        "status": status,
    }



class FakeLockCursor:
    def __init__(self, acquired):
        self.acquired = acquired
        self.last_result = None
        self.released = False

    def execute(self, sql, params=None):
        if "GET_LOCK" in sql:
            self.last_result = (1 if self.acquired else 0,)
        elif "RELEASE_LOCK" in sql:
            self.released = True
            self.last_result = (1,)

    def fetchone(self):
        return self.last_result

    def close(self):
        pass


class FakeLockConnection:
    def __init__(self, acquired):
        self.cursor_obj = FakeLockCursor(acquired)

    def cursor(self):
        return self.cursor_obj

    def close(self):
        pass


def test_d13_mysql_lock_rejects_second_sync_immediately():
    conn = FakeLockConnection(acquired=False)
    try:
        with hierarchy_service.mysql_sync_lock(connection_factory=lambda: conn):
            raise AssertionError("não deve entrar no bloco sem lock")
    except hierarchy_service.HierarchySyncAlreadyRunning:
        pass
    else:
        raise AssertionError("HierarchySyncAlreadyRunning esperado")
    assert conn.cursor_obj.released is False

def test_d13_status_lifecycle_running_to_ok_and_no_get_route():
    repo = MemoryStatusRepository()
    client = FakeOFSClient([resource("T1", "02")])

    result = hierarchy_service.sync_resource_hierarchy(
        client=client,
        root_resource_id="02",
        repository=repo,
        use_lock=False,
    )

    assert [item["status"] for item in repo.status_updates] == ["running", "ok"]
    assert repo.status_updates[-1]["success_at"] is not None
    assert repo.status_updates[-1]["finished_at"] is not None
    assert result["resources_total"] == 2
    assert all("/route" not in url.lower() for url in client.calls)


def test_d13_failure_preserves_snapshot_and_records_sanitized_error():
    repo = MemoryStatusRepository()
    repo.rows = {"OLD": {"resource_id": "OLD"}}
    client = FakeOFSClient([resource("T1", "02")], fail=True)

    try:
        hierarchy_service.sync_resource_hierarchy(
            client=client,
            root_resource_id="02",
            repository=repo,
            use_lock=False,
        )
    except requests.Timeout:
        pass
    else:
        raise AssertionError("timeout esperado")

    assert repo.replace_calls == 0
    assert repo.rows == {"OLD": {"resource_id": "OLD"}}
    assert [item["status"] for item in repo.status_updates] == ["running", "error"]
    error = repo.status_updates[-1]
    assert error["error_code"] == "TIMEOUT"
    assert "nao-deve-aparecer" not in error["error_message"]
    assert "ofs.example/private" not in error["error_message"]
    assert "<redacted>" in error["error_message"]
    assert "<url>" in error["error_message"]


def test_d13_structural_health_is_independent_and_versioned():
    now = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
    service = TechnicianMonitorService(repository=object(), hierarchy_stale_seconds=7200)
    recent = now.replace(tzinfo=None) - timedelta(minutes=10)
    payload = service._hierarchy_sync_payload(
        {
            "hierarchy": {
                "source_name": "hierarchy",
                "status": "ok",
                "last_started_at": recent,
                "last_success_at": recent,
                "last_finished_at": recent,
                "error_code": None,
                "error_message": None,
            }
        },
        now,
    )
    assert payload["state"] == "ok"
    assert payload["version"] == payload["last_success_at"]

    stale = service._hierarchy_sync_payload(
        {
            "hierarchy": {
                "source_name": "hierarchy",
                "status": "ok",
                "last_success_at": now.replace(tzinfo=None) - timedelta(hours=3),
            }
        },
        now,
    )
    assert stale["state"] == "stale"

    running = service._hierarchy_sync_payload(
        {
            "hierarchy": {
                "source_name": "hierarchy",
                "status": "running",
                "last_success_at": recent,
            }
        },
        now,
    )
    assert running["state"] == "running"


def test_d13_systemd_timer_is_hourly_persistent_and_contains_no_secrets():
    service_text = (ROOT / "deploy/systemd/ofs-resource-hierarchy-sync.service.example").read_text(encoding="utf-8")
    timer_text = (ROOT / "deploy/systemd/ofs-resource-hierarchy-sync.timer.example").read_text(encoding="utf-8")
    assert "Type=oneshot" in service_text
    assert "tools/sync_ofs_resource_hierarchy.py" in service_text
    assert "TimeoutStartSec=180" in service_text
    assert "OnBootSec=3min" in timer_text
    assert "OnUnitActiveSec=1h" in timer_text
    assert "Persistent=true" in timer_text
    combined = (service_text + timer_text).lower()
    for forbidden in ("db_password=", "client_secret=", "access_token=", "authorization: bearer", "password="):
        assert forbidden not in combined


def test_d13_ui_has_global_structure_notice_and_version_cache_invalidation_only():
    js = (ROOT / "static/js/dashboard_technicians.js").read_text(encoding="utf-8")
    template = (ROOT / "templates/dashboard_operacional.html").read_text(encoding="utf-8")
    assert "data-technicians-structure-status" in template
    assert "Atualizando estrutura de técnicos… O painel continua disponível." in js
    assert "A estrutura de técnicos não foi atualizada recentemente." in js
    assert "hierarchyVersionObserved" in js
    assert "hierarchyCacheInvalidations" in js
    assert "state.childCache.clear()" in js
    assert "state.expanded.clear()" in js
    assert 'mode: "children"' in js
    assert 'mode: "full"' not in js
    technician_block = js[js.index("function appendTechnicianDetails"):js.index("function appendAggregateDetails")]
    assert "hierarchy_sync" not in technician_block


def test_d13_monitor_keeps_three_bulk_queries_and_hierarchy_in_existing_health_query():
    source = (ROOT / "services/ofs_technician_monitor_service.py").read_text(encoding="utf-8")
    assert "metrics[\"mysql_queries_total\"] = 3" in source
    assert "'events','activities','calendars','routes','hierarchy'" in source
    assert "OFSClient" not in source
    assert "requests." not in source


def test_d13_sync_tool_and_units_do_not_reference_full_route_baseline():
    combined = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in (
            "tools/sync_ofs_resource_hierarchy.py",
            "services/ofs_resource_hierarchy_service.py",
            "deploy/systemd/ofs-resource-hierarchy-sync.service.example",
            "deploy/systemd/ofs-resource-hierarchy-sync.timer.example",
        )
    )
    assert "GET Route" not in combined
    assert "baseline-once" not in combined
    assert "ofs_technician_operational_worker.py" not in combined
