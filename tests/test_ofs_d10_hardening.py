from __future__ import annotations

from pathlib import Path

from tools.ofs_d10_hardening_probe import (
    _automatic_gate,
    _estimate_seven_day_storage,
    _health_gate,
    _static_checks,
)


ROOT = Path(__file__).resolve().parents[1]


def test_d10_static_architecture_guards_are_clean():
    checks = _static_checks()
    assert checks["js_hardcoded_ofs"] == []
    assert checks["dashboard_browser_source_has_dashboard_rows"] is False
    assert checks["status_route_uses_metadata_only_reader"] is True
    assert checks["technicians_ui_uses_mode_full"] is False
    assert checks["technicians_ui_uses_parent_id"] is True
    assert checks["technicians_polling_checks_document_hidden"] is True
    assert checks["run_forever_calls_get_route_directly"] is False
    assert checks["gitignore_instance"] is True
    assert checks["gitignore_temp_uploads"] is True



def test_d10_health_gate_accepts_uppercase_ok_from_domain_contract():
    report = {
        "direct": {
            "health": {
                "overall_integrity": "ok",
                "events_caught_up": None,
                "sources": {
                    "events": {"state": "OK", "age_seconds": 30, "threshold_seconds": 180},
                    "activities": {"state": "OK", "age_seconds": 300, "threshold_seconds": 1800},
                    "calendars": {"state": "OK", "age_seconds": 900, "threshold_seconds": 5400},
                },
            }
        }
    }

    gate = _health_gate(report)

    assert gate["status"] == "PASS"
    assert gate["overall_integrity"] == "ok"
    assert all(item["ok"] is True for item in gate["sources"].values())

def test_d10_storage_projection_scales_only_date_scoped_tables():
    retention = {
        "technicians": {"dates_total": 1},
        "activities": {"dates_total": 2},
    }
    sizes = {
        "ofs_resource_hierarchy": {"total_bytes": 100},
        "ofs_event_cursor": {"total_bytes": 10},
        "ofs_operational_sync_state": {"total_bytes": 20},
        "ofs_technician_operational_state": {"total_bytes": 1000},
        "ofs_activity_operational_state": {"total_bytes": 2000},
    }

    result = _estimate_seven_day_storage(retention, sizes)

    assert result["fixed_tables_bytes"] == 130
    assert result["date_scoped_tables"]["ofs_technician_operational_state"]["projected_7d_bytes"] == 7000
    assert result["date_scoped_tables"]["ofs_activity_operational_state"]["projected_7d_bytes"] == 7000
    assert result["projected_total_7d_bytes"] == 14130


def test_d10_automatic_gate_rejects_stale_health_or_future_rows():
    report = {
        "errors": [],
        "static": {
            "js_hardcoded_ofs": [],
            "dashboard_browser_source_has_dashboard_rows": False,
            "status_route_uses_metadata_only_reader": True,
            "technicians_ui_uses_mode_full": False,
            "technicians_ui_uses_parent_id": True,
            "technicians_polling_checks_document_hidden": True,
            "run_forever_calls_get_route_directly": False,
            "gitignore_instance": True,
            "gitignore_temp_uploads": True,
        },
        "mysql": {
            "retention_window": {
                "datasets": {
                    "technicians": {"expired_rows": 0, "future_rows": 0},
                    "activities": {"expired_rows": 0, "future_rows": 1},
                }
            }
        },
        "dashboard": {
            "snapshot": {"dashboard_rows_db": 0},
            "render": {"browser_dashboard_rows": 0},
        },
        "health_gate": {"status": "FAIL"},
    }

    gate = _automatic_gate(report)

    assert gate["status"] == "NO_GO_SIGNAL"
    assert any("materializacao futura" in item for item in gate["failures"])
    assert any("thresholds" in item for item in gate["failures"])


def test_d10_systemd_template_has_safe_worker_lifecycle_without_embedded_secrets():
    path = ROOT / "deploy/systemd/ofs-technician-operational-worker.service.example"
    text = path.read_text(encoding="utf-8")

    assert "ExecStart=__OFS_PROJECT_DIR__/venv/bin/python" in text
    assert "tools/ofs_technician_operational_worker.py" in text
    assert "Restart=on-failure" in text
    assert "KillSignal=SIGINT" in text
    assert "TimeoutStopSec=300" in text
    assert "WorkingDirectory=__OFS_PROJECT_DIR__" in text
    assert "Environment=" not in text
    assert "PASSWORD=" not in text
    assert "TOKEN=" not in text
    assert "SECRET=" not in text


def test_d10_validation_sql_is_read_only_and_checks_retention_schema_health_and_cursor_presence():
    text = (ROOT / "database/sql/20260826_ofs_d10_hardening_validate.sql").read_text(encoding="utf-8")
    upper = text.upper()

    assert "DATE_SUB(CURDATE(), INTERVAL 6 DAY)" in upper
    assert "WORK_DATE > CURDATE()" in upper
    assert "INFORMATION_SCHEMA.STATISTICS" in upper
    assert "OFS_OPERATIONAL_SYNC_STATE" in upper
    assert "CHAR_LENGTH(NEXT_PAGE)" in upper
    assert "SELECT" in upper
    for forbidden in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "DROP ", "ALTER "):
        assert forbidden not in upper


def test_d10_probe_declares_no_oracle_ofs_scope_and_no_direct_http_client():
    text = (ROOT / "tools/ofs_d10_hardening_probe.py").read_text(encoding="utf-8")

    assert "local_mysql_flask_only_no_oracle_ofs_calls" in text
    assert "OFSOperationalAPI(" not in text
    assert "requests.get(" not in text
    assert "requests.request(" not in text


def test_d10_worker_has_success_observability_without_cursor_or_subscription_values():
    text = (ROOT / "services/ofs_technician_operational_service.py").read_text(encoding="utf-8")

    assert "Worker operacional iniciado" in text
    assert "Baseline operacional concluído" in text
    assert "Activities reconciliado" in text
    assert "Calendars reconciliado" in text
    assert "Events reconciliado" in text
    assert "Recuperação de subscription concluída" in text
    assert 'LOGGER.info("%s", subscription_id)' not in text
    assert 'LOGGER.info("%s", next_page)' not in text


def test_d10_worker_cli_handles_first_sigint_as_graceful_stop_request():
    text = (ROOT / "tools/ofs_technician_operational_worker.py").read_text(encoding="utf-8")

    assert "threading.Event()" in text
    assert "signal.SIGINT" in text
    assert "stop_event.set()" in text
    assert "collector.run_forever(stop_predicate=stop_event.is_set)" in text
    assert "encerramento solicitado; aguardando ciclo em andamento finalizar" in text
    assert "encerrado graciosamente" in text
    assert "if interrupt_count == 1" in text
    assert "raise KeyboardInterrupt" in text
