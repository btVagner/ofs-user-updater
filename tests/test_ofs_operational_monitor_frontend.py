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
    assert "data-table-title" in template
    assert "data-table-note" not in template
    assert "data-table-note" not in script


def test_frontend_has_multi_bucket_and_status_checkboxes():
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    assert "data-bucket-filter" in template
    assert "data-bucket-options" in template
    assert "data-status-filter" in template
    assert "data-status-options" in template
    assert "data-status-summary" in template
    assert "selectedBuckets: new Set()" in script
    assert 'input.type = "checkbox"' in script
    assert "state.selectedBuckets.has(row.area)" in script
    assert "excludedStatusesByView: new Map()" in script
    assert "state.excludedStatusesByView.get(state.view)" in script
    assert "excludedStatuses.has(normalized(row.status))" in script
    assert 'renderMultiCheckboxes(statusFilter' in script
    assert 'input.indeterminate = selectedCount > 0 && selectedCount < options.length' in script
    assert 'selectedCount === 0 ? noneLabel' in script
    assert 'filter.hidden = options.length === 0' in script
    assert "previousOptions.some((item) => excluded.has(item.key))" in script
    assert "state.excludedStatusesByView.clear()" in script
    assert "const rows = currentRows();" in script


def test_activity_type_filter_supports_multiple_choices_from_snapshot():
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    assert 'data-activity-type-filter' in template
    assert 'data-activity-type-options' in template
    assert 'data-activity-type-summary' in template
    assert 'new Set(["late", "slot", "black"])' in script
    assert 'excludedActivityTypesByView: new Map()' in script
    assert 'state.excludedActivityTypesByView.get(state.view)' in script
    assert 'excludedActivityTypes.has(normalized(row.type))' in script
    assert 'if (activityTypeFilter) activityTypeFilter.hidden = !eligible' in script
    assert 'renderActivityTypeOptions(sourceRows, view)' in script
    assert 'renderMultiCheckboxes(activityTypeFilter' in script
    assert '"Todos os tipos", "Nenhum tipo", "tipos selecionados"' in script
    assert 'state.excludedActivityTypesByView.clear()' in script
    assert 'const rows = currentRows();' in script


def test_frontend_has_shared_uf_toggle_filter_for_every_view():
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    assert "Estado (UF)" in template
    assert "data-state-options" in template
    assert "selectedStates: new Set()" in script
    assert "VIEW_KEYS.flatMap" in script
    assert 'if (!container) return;' in script
    assert 'button.setAttribute("aria-pressed", String(selected))' in script
    assert "state.selectedStates.has(value)" in script


def test_migration_creates_shared_cache_lock_support_and_permission():
    sql = (ROOT / "database" / "sql" / "20260925_ofs_operational_monitor_apply.sql").read_text(encoding="utf-8")
    service = (ROOT / "services" / "ofs_operational_monitor_service.py").read_text(encoding="utf-8")
    assert "ofs_operational_monitor_snapshot" in sql
    assert "ofs_operational_monitor_refresh_log" in sql
    assert "ofs.monitor_operacional" in sql
    assert "customer_state" in sql
    assert "SNAPSHOT_TTL_SECONDS = 10 * 60" in service
    assert "GET_LOCK(%s,0)" in service


def test_treatment_ui_is_local_guarded_and_excludes_black():
    template = (ROOT / "templates" / "ofs_operational_monitor.html").read_text(encoding="utf-8")
    script = (ROOT / "static" / "js" / "ofs_operational_monitor.js").read_text(encoding="utf-8")
    routes = (ROOT / "routes" / "ofs_operational_monitor_routes.py").read_text(encoding="utf-8")
    sql = (ROOT / "database" / "sql" / "20260928_ofs_operational_monitor_treatment_apply.sql").read_text(encoding="utf-8")
    assert "data-treatment-dialog" in template
    assert "data-treatment-filter" in template
    assert "data-refresh-treatments" in template
    assert 'new Set(["late", "idle", "notStarted", "slot"])' in script
    assert "loadTreatments" in script and "renewTreatment" in script
    assert '["Tratativa", "Ação", ...HEADERS[view]]' in script
    assert '["Tratativa", "Responsável", ...HEADERS[state.view]]' in script
    treatment_loader = script.split("async function loadTreatments()", 1)[1].split("async function postTreatment", 1)[0]
    assert "await loadSnapshot()" not in treatment_loader
    assert '"X-Monitor-CSRF"' in script
    assert "_csrf_valid()" in routes
    assert "ofs.monitor_operacional.tratar" in sql
    assert "ofs.monitor_operacional.supervisionar" in sql
    assert "/rest/ofsc" not in script.lower()


def test_supervision_is_separate_permission_and_configurable():
    template = (ROOT / "templates" / "ofs_operational_monitor_supervision.html").read_text(encoding="utf-8")
    script = (ROOT / "static" / "js" / "ofs_operational_monitor_supervision.js").read_text(encoding="utf-8")
    assert '<option value="hour">1h</option>' in template
    assert '<option value="six_hours">6h</option>' in template
    assert '<option value="day">Último dia</option>' in template
    assert '<option value="week">7 dias</option>' in template
    assert "Top 20" in template
    assert "data-start" in template and "data-end" in template
    assert "data-ranking" in template and "data-recent" in template
    assert "data-active-total" in template and "data-waiting-total" in template
    assert "data-action" in template and "data-agent" in template
    assert "om-ranking-track" in script and "lease_expires_at" in script
    assert 'params.set("action"' in script and 'params.set("agent"' in script
    assert "/rest/ofsc" not in script.lower()
