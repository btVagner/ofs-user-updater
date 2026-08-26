from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "templates" / "dashboard_operacional.html"
JS = ROOT / "static" / "js" / "dashboard_technicians.js"
CSS = ROOT / "static" / "css" / "dashboard_technicians.css"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def test_dashboard_exposes_local_urls_without_embedding_technician_payload():
    template = read(TEMPLATE)

    assert "data-technicians-summary-url=\"{{ url_for('technician_monitor_summary') }}\"" in template
    assert "data-technicians-tree-url=\"{{ url_for('technician_monitor_tree') }}\"" in template
    assert 'id="dashboard-view-technicians" class="dashboard-technicians-view hidden"' in template
    assert "technicians-payload" not in template
    assert "mode=full" not in template
    assert "dashboard_technicians.js" in template
    assert "css/dashboard_technicians.css" in template


def test_first_load_is_bound_to_technicians_tab_and_requests_summary_and_root_in_parallel():
    js = read(JS)

    assert 'button.addEventListener("click"' in js
    assert 'setActivePanel(button.dataset.dashboardView)' in js
    assert 'if (!state.initialized) {' in js
    assert "loadInitial();" in js
    assert "Promise.allSettled([" in js
    assert "loadSummary()," in js
    assert "loadChildren(null, { force: true })," in js
    assert 'state.initialized = true;' in js

    # O carregamento inicial não é executado solto no final do DOMContentLoaded.
    assert js.rstrip().endswith("});")
    assert js.count("loadInitial();") == 1


def test_ui_never_requests_full_tree_and_expansion_uses_parent_id():
    js = read(JS)

    assert 'mode: "children"' in js
    assert 'parent_id: parentId' in js
    assert 'mode: "full"' not in js
    assert "mode=full" not in js
    assert "state.childCache.has(key)" in js
    assert "state.childCache.set(key, nodes)" in js
    assert "state.expanded.has(resourceId)" in js
    assert "state.expanded.add(resourceId)" in js
    assert "state.expanded.delete(resourceId)" in js


def test_only_problems_is_server_propagated_and_other_filters_use_loaded_aggregates():
    js = read(JS)

    assert 'only_problems: state.onlyProblems ? 1 : null' in js
    assert 'nextFilter === "problems"' in js
    assert 'aggregates.waiting_route_count' in js
    assert 'aggregates.active_route_count' in js
    assert 'aggregates.open_activity_count' in js
    assert "state.childCache.clear()" in js


def test_loaded_node_search_does_not_require_full_tree():
    template = read(TEMPLATE)
    js = read(JS)

    assert "Buscar nos nós carregados" in template
    assert "A busca considera somente os ramos já carregados" in template
    assert 'searchInput.addEventListener("input", renderTree)' in js
    assert "state.childCache.get(cacheKey(id))" in js


def test_integrity_stale_and_alert_are_rendered_as_independent_badges():
    js = read(JS)

    assert '"[ALERTA] Alerta"' in js
    assert '"[?] Dados desatualizados"' in js
    assert "appendChild(createBadge(severity.label" in js
    assert "appendChild(createBadge(integrity.label" in js
    assert "operational_severity" in js
    assert "[?] Situação operacional incerta" in js
    assert "[?] Estado desconhecido" not in js
    assert "integrity_state" in js


def test_caught_up_null_is_never_coerced_to_true():
    js = read(JS)

    assert "const caughtUp = health.events_caught_up;" in js
    assert "caughtUp === true || caughtUp === false" in js
    assert "Boolean(health.events_caught_up)" not in js
    assert "!!health.events_caught_up" not in js


def test_technician_without_state_and_empty_nodes_have_visible_semantics():
    js = read(JS)

    assert '"Sem estado"' in js
    assert '"Jornada desconhecida"' in js
    assert '"Rota desconhecida"' in js
    assert '"Nó sem filhos."' in js
    assert "summary.data_available === false" in js


def test_polling_runs_only_for_active_visible_tab_and_refreshes_loaded_open_parents():
    js = read(JS)

    assert "const POLL_INTERVAL_MS = 60000;" in js
    assert "if (!state.active || document.hidden || !state.initialized) return;" in js
    assert 'document.addEventListener("visibilitychange"' in js
    assert "if (document.hidden)" in js
    assert "stopPolling();" in js
    assert "refreshControlled();" in js
    assert "const parents = [null, ...Array.from(state.expanded)];" in js
    assert "MAX_REFRESH_CONCURRENCY = 3" in js


def test_tree_uses_single_delegated_listener_and_cache_prevents_reopen_fetch():
    js = read(JS)

    assert js.count('tree.addEventListener("click"') == 1
    assert "if (!state.childCache.has(key))" in js
    assert "await loadChildren(resourceId, { force: false })" in js
    assert "tree.replaceChildren();" in js


def test_errors_are_user_safe_and_fetch_has_timeout():
    js = read(JS)

    assert "const REQUEST_TIMEOUT_MS = 15000;" in js
    assert "controller.abort();" in js
    assert "READ_MODEL_ERROR" not in js  # frontend não precisa interpretar detalhes internos do backend
    assert "traceback" not in js.lower()
    assert "sql select" not in js.lower()


def test_no_hardcoded_prefix_or_external_ofs_calls_in_frontend():
    js = read(JS)

    forbidden = (
        "/ofs/",
        "OFSClient",
        "oracle.com",
        "/rest/ofscCore/",
        "requests.get",
        "requests.post",
        "mode=full",
    )
    for value in forbidden:
        assert value not in js


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js não disponível")
def test_dashboard_technicians_javascript_parses_with_node():
    result = subprocess.run(
        [shutil.which("node"), "--check", str(JS)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_css_has_responsive_and_keyboard_focus_contracts():
    css = read(CSS)

    assert "@media (max-width: 720px)" in css
    assert ".technicians-tree-toggle:focus-visible" in css
    assert ".dashboard-view-tabs button:focus-visible" in css
    assert ".technicians-badge.alert" in css
    assert ".technicians-badge.stale" in css
