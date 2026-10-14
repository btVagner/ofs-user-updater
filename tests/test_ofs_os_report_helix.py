"""Report choices, validation and export transport with no real DB or OFS."""
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask
from openpyxl import load_workbook

from services import ofs_os_report_service as service
from routes import relatorios_routes as routes


HELIX = {
    "B2B_HLX": "Ativação B2B Helix",
    "MIG_PLA_B2B_HELIX": "Migração de Plano B2B Helix",
    "MUD_END_B2B_HLX": "Mudança de Endereço B2B Helix",
    "RET_B2B_HLX": "Retirada B2B Helix",
    "SOL_SER_HLX": "Solicitação de Serviço Helix",
    "SUP_HLX": "Suporte Helix",
}


@pytest.fixture
def catalog(monkeypatch):
    rows = [
        {"code": "INST", "label": "Instalação", "category": "customer_home", "is_internal": False},
        {"code": "ALM", "label": "Almoço", "category": "internal", "is_internal": True},
        {"code": "INF_COR", "label": "Infra corretiva", "category": "redes", "is_internal": False},
    ]
    monkeypatch.setattr(service, "list_activity_types", lambda: rows)
    monkeypatch.setattr(service, "_validate_resources", lambda values: values)
    return rows


def payload(types):
    return {"dateFrom": "2026-10-14", "dateTo": "2026-10-14", "activityTypes": types,
            "statuses": ["pending"], "fields": ["activityId", "activityType"], "resources": ["02"]}


@pytest.mark.parametrize("code,label", HELIX.items())
def test_helix_choice_can_be_exported_using_exact_api_code(catalog, code, label):
    choices = {item["code"]: item for item in service.list_ofs_os_activity_types()}
    assert choices[code]["label"] == label
    assert choices[code]["is_internal"] is False
    assert service.validate_ofs_os_report_payload(payload([code]))["activity_types"] == [code]


def test_helix_choices_are_unique_and_do_not_change_shared_catalog(catalog):
    catalog.append({"code": "SUP_HLX", "label": "Nome antigo", "category": "internal", "is_internal": True})
    choices = service.list_ofs_os_activity_types()
    assert len([item for item in choices if item["code"] == "SUP_HLX"]) == 1
    assert next(item for item in choices if item["code"] == "SUP_HLX")["label"] == HELIX["SUP_HLX"]
    assert catalog[-1]["label"] == "Nome antigo"
    assert "INF_COR" not in {item["code"] for item in choices}


def test_existing_report_category_guards_remain(catalog):
    assert service.list_redes_activity_types() == [catalog[2]]
    assert not set(HELIX) & service._valid_activity_type_codes()
    with pytest.raises(ValueError, match="Redes"):
        service.validate_ofs_os_report_payload(payload(["INF_COR", "B2B_HLX"]))
    with pytest.raises(ValueError, match="tipo de atividade"):
        service.validate_redes_report_payload(payload(list(HELIX)))
    with pytest.raises(ValueError, match="tipo de atividade"):
        service.validate_thermometer_report_payload(payload(list(HELIX)))


def test_report_page_renders_names_and_checkbox_codes(catalog, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    app = Flask(__name__, template_folder=str(root / "templates"), static_folder=str(root / "static"))
    app.secret_key = "unit-test"
    app.testing = True
    monkeypatch.setattr(routes, "list_resources_grouped", lambda: {key: [] for key in service.RESOURCE_TYPES})
    routes.init_app(app)
    app.jinja_env.globals["has_perm"] = routes.has_perm
    for name in ("templates/includes/navbar.html", "templates/includes/head.html"):
        for endpoint in re.findall(r"url_for\('([^']+)'", (root / name).read_text(encoding="utf-8")):
            if endpoint not in app.view_functions:
                app.add_url_rule("/fixture/" + endpoint, endpoint, lambda: "fixture")
    client = app.test_client()
    with client.session_transaction() as session:
        session.update(usuario_logado="fixture", nome_usuario="Fixture", tipo_id=2,
                       permissoes=["relatorios.acessar"])
    response = client.get("/relatorios/ofs-os")
    assert response.status_code == 200
    for code, label in HELIX.items():
        assert re.search(r'name="activityTypes" value="' + code + r'"\s+data-category="b2b_helix" checked', response.text)
        assert "<span>" + label + "</span>" in response.text


def test_export_query_uses_codes_and_xlsx_uses_names(catalog, monkeypatch, tmp_path):
    config = service.validate_ofs_os_report_payload(payload(list(HELIX)))
    calls = []
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return {"items": [{"activityId": str(index), "activityType": code, "status": "pending",
                               "date": "2026-10-14"} for index, code in enumerate(HELIX, 1)], "hasMore": False}
    def fake_get(url, **kwargs):
        calls.append(kwargs["params"])
        return Response()
    monkeypatch.setattr(service.requests, "get", fake_get)
    monkeypatch.setattr(service, "_load_resource_name_map", lambda: {})
    monkeypatch.setattr(service, "_write_job_status", lambda *_: None)
    spool = tmp_path / "rows.jsonl"
    result = service._fetch_activities(SimpleNamespace(base_url="https://fixture.invalid", auth=None),
                                      config, str(tmp_path), "fixture", {}, str(spool))
    query = dict(calls[0])["q"]
    for code, label in HELIX.items():
        assert "activityType=='" + code + "'" in query
        assert label not in query
    assert len(calls) == 1
    assert [json.loads(line)[1] for line in spool.read_text(encoding="utf-8").splitlines()] == list(HELIX.values())
    output = tmp_path / "helix.xlsx"
    service._build_xlsx(str(spool), config["fields"], str(output), result["column_widths"])
    workbook = load_workbook(output, read_only=True)
    try:
        rows = list(workbook.active.values)
        assert [row[1] for row in rows[1:]] == list(HELIX.values())
    finally:
        workbook.close()
