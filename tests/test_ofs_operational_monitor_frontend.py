from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_monitor_is_permission_routed_and_uses_local_snapshot_endpoints():
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    home = (ROOT / "templates" / "home.html").read_text(encoding="utf-8")
    navbar = (ROOT / "templates" / "includes" / "navbar.html").read_text(encoding="utf-8")
    routes = (ROOT / "routes" / "ofs_operational_monitor_routes.py").read_text(encoding="utf-8")
    assert "ofs.monitor_operacional" in home
    assert "ofs.monitor_operacional" in navbar
    assert "@perm_required(PERMISSION)" in routes
    assert "data-data-url" in template and "data-refresh-url" in template
    assert "ClientSecret" not in template


def test_frontend_never_calls_oracle_and_has_all_plugin_views():
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    assert "/rest/ofsc" not in script.lower()
    assert "clientsecret" not in script.lower()
    assert "fetch(root.dataset.refreshUrl" in script
    for view in ("OS em alerta", "Técnicos sem OS", "Rotas não iniciadas", "Fora do turno", "Clientes Black"):
        assert view in template


def test_frontend_has_multi_bucket_checkboxes_and_status_filter():
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    assert "data-bucket-filter" in template
    assert "data-bucket-options" in template
    assert "data-status" in template
    assert "selectedBuckets: new Set()" in script
    assert 'input.type = "checkbox"' in script
    assert "state.selectedBuckets.has(row.area)" in script
    assert "normalized(row.status) !== status" in script


def test_migration_creates_shared_cache_lock_support_and_permission():
    sql = (ROOT / "database" / "sql" / "20260925_ofs_operational_monitor_apply.sql").read_text(encoding="utf-8")
    service = (ROOT / "services" / "ofs_operational_monitor_service.py").read_text(encoding="utf-8")
    assert "ofs_operational_monitor_snapshot" in sql
    assert "ofs_operational_monitor_refresh_log" in sql
    assert "ofs.monitor_operacional" in sql
    assert "SNAPSHOT_TTL_SECONDS = 10 * 60" in service
    assert "GET_LOCK(%s,0)" in service
