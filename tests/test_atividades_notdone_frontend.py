from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8-sig")


def test_notdone_page_preserves_both_operational_views_and_actions():
    template = read("templates/atividades_notdone.html")

    assert "atividades_notdone_tratadas" in template
    assert "atividades_notdone_tratar" in template
    assert "atividades_notdone_revogar" in template
    assert 'data-view-mode="{{ view_mode }}"' in template
    assert "Finalizar tratativa" in template


def test_notdone_page_exposes_server_side_state_filter_in_both_views():
    template = read("templates/atividades_notdone.html")
    javascript = read("static/js/atividades_notdone.js")

    assert 'id="notdoneState"' in template
    assert 'name="state"' in template
    assert 'data-col="state">UF' in template
    assert template.index('data-col="bucket"') < template.index('data-col="state">UF')
    assert 'class="td-state"' in template
    assert 'id="m_stateProvince"' in template
    assert 'setVal("m_stateProvince", a.stateProvince);' in javascript
    assert "state=state_filter or none" in template


def test_notdone_page_has_clear_hierarchy_and_accessible_filters():
    template = read("templates/atividades_notdone.html")

    assert 'class="page-header-copy"' in template
    assert 'class="page-eyebrow"' in template
    assert 'class="filter-card-heading"' in template
    assert 'for="notdoneDateFrom"' in template
    assert 'for="notdoneDateTo"' in template
    assert 'for="notdoneResources"' in template
    assert "Códigoda OS" not in template
    assert "Código da OS" in template


def test_notdone_styles_cover_modal_table_and_responsive_states():
    css = read("static/css/atividades_notdone.css")

    assert "--notdone-surface:" in css
    assert 'html[data-theme="dark"] body[data-view-mode]' in css
    assert "position: sticky;" in css
    assert ".modal-grid input[readonly]" in css
    assert ".pagination-wrap" in css
    assert "@media (max-width: 640px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
    assert ".td-state" in css
