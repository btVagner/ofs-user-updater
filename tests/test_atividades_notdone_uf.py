from pathlib import Path

from routes.atividades_notdone_routes import (
    BRAZIL_STATE_CODES,
    NOTDONE_SOURCE_UPSERT_SQL,
    build_notdone_source_values,
    normalize_state_filter,
    normalize_state_value,
)


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8-sig")


def test_state_normalization_accepts_brazilian_codes_only_for_filtering():
    assert normalize_state_value(" sp ") == "SP"
    assert normalize_state_value(None) is None
    assert normalize_state_filter(" mg ") == "MG"
    assert normalize_state_filter("XX") == ""
    assert len(BRAZIL_STATE_CODES) == 27


def test_import_requests_and_upserts_state_without_touching_treatment_fields():
    source = read("routes/atividades_notdone_routes.py")

    assert '"stateProvince"' in source
    assert "state_province = COALESCE(VALUES(state_province), state_province)" in NOTDONE_SOURCE_UPSERT_SQL
    assert "ON DUPLICATE KEY UPDATE" in NOTDONE_SOURCE_UPSERT_SQL
    assert "tratativa_status =" not in NOTDONE_SOURCE_UPSERT_SQL
    assert " AND state_province = %s" in source


def test_api_activity_values_normalize_state_and_keep_expected_order():
    values = build_notdone_source_values({
        "activityId": 123,
        "activityType": " sup_rep ",
        "city": "São Paulo",
        "stateProvince": " sp ",
        "customerName": "Cliente",
        "date": "2026-09-27",
    })

    assert len(values) == 13
    assert values[0] == "123"
    assert values[1] == "SUP_REP"
    assert values[2] == "São Paulo"
    assert values[3] == "SP"
    assert values[6] == "Cliente"
    assert values[12] == "2026-09-27"


def test_state_schema_migration_is_idempotent_and_indexed():
    apply_sql = read("database/sql/20260927_ofs_atividades_notdone_uf_apply.sql")
    validate_sql = read("database/sql/20260927_ofs_atividades_notdone_uf_validate.sql")

    assert "information_schema.columns" in apply_sql
    assert "ADD COLUMN state_province VARCHAR(64) NULL" in apply_sql
    assert "idx_notdone_date_state_treated" in apply_sql
    assert "idx_notdone_date_state_treated" in validate_sql
    assert "rows_with_state" in validate_sql
