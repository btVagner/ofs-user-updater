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


def test_dashboard_keeps_local_lazy_contract_and_no_full_tree():
    template = read(TEMPLATE)
    js = read(JS)
    assert "data-technicians-summary-url=\"{{ url_for('technician_monitor_summary') }}\"" in template
    assert "data-technicians-tree-url=\"{{ url_for('technician_monitor_tree') }}\"" in template
    assert "technicians-payload" not in template
    assert 'mode: "children"' in js
    assert 'parent_id: parentId' in js
    assert 'mode: "full"' not in js
    assert "mode=full" not in js


def test_d12_refresh_copy_is_local_only_and_does_not_call_ofs():
    template = read(TEMPLATE)
    js = read(JS)
    assert "Atualizar leitura" in template
    assert "Recarrega os dados locais. A sincronização com o OFS é automática." in template
    assert 'refreshButton.addEventListener("click", () => refreshControlled())' in js
    for forbidden in ("OFSClient", "oracle.com", "/rest/ofscCore/", "/ofs/"):
        assert forbidden not in js


def test_freshness_is_global_and_not_rendered_per_technician_or_cluster():
    js = read(JS)
    assert "Sincronização automática desatualizada" in js
    assert "technicians-health-sources" in js
    assert "integrity_state" not in js[js.index("function appendTechnicianDetails"):js.index("function appendAggregateDetails")]
    aggregate_block = js[js.index("function appendAggregateDetails"):js.index("function createTreeNode")]
    assert "integrity_degraded_count" not in aggregate_block
    assert "dados degradados" not in js
    assert "Integridade degradada" not in js
    assert "Sem estado operacional do dia" in js


def test_alert_codes_have_human_labels_and_raw_codes_are_not_used_as_visible_text():
    js = read(JS)
    assert 'ROUTE_NOT_STARTED: "Rota não iniciada no horário"' in js
    assert 'ROUTE_ACTIVE_AFTER_SHIFT: "Rota ativa após o fim da jornada"' in js
    assert 'ACTIVITY_OPEN_AFTER_SHIFT: "OS em atendimento após o fim da jornada"' in js
    assert "message.textContent = alertLabel(code);" in js
    assert "message.textContent = code" not in js
    assert "createBadge(code" not in js


def test_kpis_are_clickable_uniform_and_cover_required_filters():
    js = read(JS)
    css = read(CSS)
    for filter_name in ("alert", "attention", "waiting_route", "active_route", "started", "suspended", "open"):
        assert f'{filter_name}: "{filter_name}"' in js
    assert "data.techniciansKpiFilter" not in js
    assert "dataset.techniciansKpiFilter" in js
    assert "aria-pressed" in js
    assert "button.technicians-stat.active" in css
    assert "min-height: 82px" in css
    assert '"kpi-alert"' in js
    assert '.technicians-stat.kpi-alert' in css
    assert '.technicians-stat.alert {' not in css


def test_kpi_filter_is_server_side_and_propagated_to_expansions():
    js = read(JS)
    assert "filter: state.serverFilter" in js
    assert "only_problems: state.onlyProblems ? 1 : null" in js
    assert "state.childCache.clear()" in js
    assert "await loadChildren(resourceId, { force: false })" in js
    assert "aggregates.waiting_route_count" not in js


def test_cluster_view_uses_backend_aggregates_and_click_locates_node():
    template = read(TEMPLATE)
    js = read(JS)
    assert "Visão por cluster" in template
    assert "Programados = técnicos WORKING no dia" in template
    assert "cluster.working_count" in js
    assert "cluster.active_route_count" in js
    assert "cluster.activation_percent" in js
    assert 'percent.textContent = cluster.activation_percent === null' in js
    assert "await locateCluster" in js
    assert "scrollIntoView" in js


def test_timestamp_display_uses_one_explicit_timezone():
    js = read(JS)
    assert 'const DISPLAY_TIMEZONE = "America/Sao_Paulo";' in js
    assert "timeZone: DISPLAY_TIMEZONE" in js
    assert "Leitura gerada em" in js


def test_tree_rows_are_compact_columns_not_badge_clouds():
    js = read(JS)
    css = read(CSS)
    assert "technicians-node-grid" in js
    assert 'fact("Jornada"' in js
    assert 'fact("Rota"' in js
    assert 'fact("OS abertas"' in js
    assert 'fact("Em atendimento"' in js
    assert ".technicians-node-details.technicians-node-grid" in css
    assert ".technicians-node-row .technicians-badge" in css


def test_loaded_node_search_stays_local_and_polling_only_when_visible():
    template = read(TEMPLATE)
    js = read(JS)
    assert "Buscar nos nós carregados" in template
    assert 'searchInput.addEventListener("input", renderTree)' in js
    assert "const POLL_INTERVAL_MS = 60000;" in js
    assert "if (!state.active || document.hidden || !state.initialized) return;" in js
    assert 'document.addEventListener("visibilitychange"' in js


def test_caught_up_null_is_never_coerced_to_true():
    js = read(JS)
    assert "const caughtUp = health.events_caught_up;" in js
    assert "caughtUp === true || caughtUp === false" in js
    assert "!!health.events_caught_up" not in js


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js não disponível")
def test_dashboard_technicians_javascript_parses_with_node():
    result = subprocess.run([shutil.which("node"), "--check", str(JS)], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_css_keeps_responsive_and_keyboard_accessibility():
    css = read(CSS)
    assert "@media (max-width: 720px)" in css
    assert ".technicians-tree-toggle:focus-visible" in css
    assert ".technicians-cluster-link:focus-visible" in css
    assert "button.technicians-stat:focus-visible" in css


def test_d12_summary_hides_unused_cards_and_uses_working_label():
    js = JS.read_text(encoding="utf-8")
    assert '["Sem estado",' not in js
    assert '["OS suspensas",' not in js
    assert '"Em jornada hoje"' in js
    assert 'fact("Em jornada"' in js


def test_d12_route_active_aggregate_has_positive_style():
    js = JS.read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    assert 'fact("Rota ativa", formatNumber(a.active_route_count), "positive")' in js
    assert ".technicians-fact.positive" in css
