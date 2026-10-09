from urllib.parse import parse_qs, urlparse

import pytest
from flask import Flask

import routes.atividades_notdone_routes as notdone_routes

from routes.atividades_notdone_routes import (
    NOTDONE_SOURCE_UPSERT_SQL,
    build_notdone_source_values,
    fetch_relevant_notdone_activities,
    is_relevant_notdone_activity,
)


@pytest.mark.parametrize(
    ("activity", "expected"),
    [
        ({"status": "notdone"}, True),
        ({"status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "998"}, True),
        ({"status": "cancelled", "XA_ORG_SYS": " adapter ", "XA_SER_CLO_IMP_ADA": 999}, True),
        ({"status": "cancelled", "XA_ORG_SYS": "ADAPTER", "XA_SER_CLO_IMP_ADA": "99999"}, True),
        ({"status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "52482"}, True),
        ({"status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": 52482}, True),
        ({"status": "cancelled", "XA_ORG_SYS": "Adapter", "_XA_SER_CLO_IMP_ADA": "998"}, False),
        ({"status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "997"}, False),
        ({"status": "cancelled", "XA_ORG_SYS": "NG", "XA_SER_CLO_IMP_ADA": "998"}, False),
        ({"status": "cancelled", "XA_ORG_SYS": "NG", "XA_SER_CLO_IMP_ADA": "52482"}, False),
        ({"status": "cancelled", "XA_ORG_SYS": "HELIX", "XA_SER_CLO_IMP_ADA": "99999"}, False),
        ({"status": "cancelled", "XA_SER_CLO_IMP_ADA": "999"}, False),
        ({"status": "completed", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "998"}, False),
    ],
)
def test_only_notdone_and_matching_adapter_cancellations_are_eligible(activity, expected):
    assert is_relevant_notdone_activity(activity) is expected


def test_adapter_reason_uses_existing_database_column_without_changing_treatment():
    values = build_notdone_source_values({
        "activityId": "A1",
        "status": "cancelled",
        "XA_ORG_SYS": "Adapter",
        "XA_SER_CLO_IMP_ADA": 99999,
    })

    assert values[0] == "A1"
    assert values[1] == "cancelled"
    assert values[11] == "99999"
    assert "activity_status = VALUES(activity_status)" in NOTDONE_SOURCE_UPSERT_SQL
    assert "tratativa_status =" not in NOTDONE_SOURCE_UPSERT_SQL
    assert "tratado_em =" not in NOTDONE_SOURCE_UPSERT_SQL


def test_status_schema_defaults_legacy_rows_to_notdone_and_can_be_validated():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    apply_sql = (root / "database/sql/20261009_ofs_atividades_notdone_status_apply.sql").read_text(encoding="utf-8")
    validate_sql = (root / "database/sql/20261009_ofs_atividades_notdone_status_validate.sql").read_text(encoding="utf-8")
    assert "information_schema.columns" in apply_sql
    assert "activity_status VARCHAR(16) NOT NULL DEFAULT ''notdone''" in apply_sql
    assert "GROUP BY activity_status" in validate_sql


def test_fetch_uses_selected_parent_and_paginates_each_status_independently():
    class FakeClient:
        base_url = "https://example.invalid/rest/ofscCore/v1"

        def __init__(self):
            self.queries = []

        def authenticated_get(self, url):
            query = parse_qs(urlparse(url).query)
            self.queries.append(query)
            if query["q"] == ["status=='notdone'"]:
                return {"items": [{"activityId": "N1", "status": "notdone"}], "hasMore": False}
            if query["offset"] == ["0"]:
                return {
                    "items": [
                        {"activityId": "C0", "status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "997"},
                        {"activityId": "C1", "status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "998"},
                    ],
                    "hasMore": True,
                }
            return {
                "items": [{"activityId": "C2", "status": "cancelled", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "99999"}],
                "hasMore": False,
            }

    client = FakeClient()
    items = fetch_relevant_notdone_activities(
        client, "2026-10-09", "2026-10-09", "MG", max_pages=3, page_limit=2
    )

    assert [item["activityId"] for item in items] == ["N1", "C1", "C2"]
    assert [query["offset"] for query in client.queries] == [["0"], ["0"], ["2"]]
    assert all(query["resources"] == ["MG"] for query in client.queries)
    assert all(query["dateFrom"] == ["2026-10-09"] for query in client.queries)
    assert "XA_ORG_SYS" not in client.queries[0]["fields"][0]
    assert "XA_ORG_SYS" in client.queries[1]["fields"][0]
    assert "_XA_SER_CLO_IMP_ADA" not in client.queries[1]["fields"][0]


def test_fetch_fails_closed_when_cancelled_pages_are_truncated():
    class EndlessClient:
        base_url = "https://example.invalid"

        def authenticated_get(self, url):
            query = parse_qs(urlparse(url).query)
            if query["q"] == ["status=='notdone'"]:
                return {"items": [], "hasMore": False}
            return {"items": [{"status": "cancelled"}], "hasMore": True}

    with pytest.raises(ValueError, match="importação cancelada"):
        fetch_relevant_notdone_activities(EndlessClient(), "2026-10-09", "2026-10-09", "MG", max_pages=1)


def test_import_saves_only_eligible_cancelled_and_keeps_notdone(monkeypatch):
    saved = []
    committed = []

    class FakeCursor:
        rowcount = 1

        def execute(self, sql, values):
            saved.append((sql, values))

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            committed.append(True)

        def close(self):
            pass

    class FakeClient:
        base_url = "https://example.invalid"

        def authenticated_get(self, url):
            query = parse_qs(urlparse(url).query)
            if query["q"] == ["status=='notdone'"]:
                return {"items": [{"activityId": "N1", "status": "notdone", "activityType": "SUP_REP"}], "hasMore": False}
            return {
                "items": [
                    {"activityId": "C1", "status": "cancelled", "activityType": "SUP_REP", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "998"},
                    {"activityId": "C2", "status": "cancelled", "activityType": "SUP_REP", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "997"},
                    {"activityId": "C3", "status": "cancelled", "activityType": "SUP_REP", "XA_ORG_SYS": "NG", "XA_SER_CLO_IMP_ADA": "999"},
                    {"activityId": "C4", "status": "cancelled", "activityType": "RET", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": "52482"},
                    {"activityId": "C5", "status": "cancelled", "activityType": "SUP_REP", "XA_ORG_SYS": "Adapter", "XA_SER_CLO_IMP_ADA": 52482},
                ],
                "hasMore": False,
            }

    monkeypatch.setattr(notdone_routes, "OFSClient", FakeClient)
    monkeypatch.setattr(notdone_routes, "get_connection", FakeConnection)
    app = Flask(__name__)
    app.secret_key = "test-only"
    notdone_routes.init_app(app)

    with app.test_client() as client:
        with client.session_transaction() as session:
            session["usuario_logado"] = "test-user"
            session["permissoes"] = ["ofs.atividades_notdone"]
        response = client.post("/atividades-notdone/importar", data={
            "dateFrom": "2026-10-09", "dateTo": "2026-10-09", "resources": "MG",
        })
        with client.session_transaction() as session:
            flashes = session.get("_flashes", [])

    assert response.status_code == 302
    assert [values[0] for _, values in saved] == ["N1", "C1", "C5"]
    assert saved[0][1][1] == "notdone"
    assert saved[1][1][1] == "cancelled"
    assert saved[1][1][11] == "998"
    assert saved[2][1][11] == "52482"
    assert committed == [True]
    assert any("Desconsiderados por tipo: 1" in message for _, message in flashes)
    assert any("Canceladas Adapter processadas: 2" in message for _, message in flashes)
