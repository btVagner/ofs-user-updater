from __future__ import annotations

from pathlib import Path

from tools.build_d14_release_package import _is_excluded
from tools.ofs_d14_release_probe import (
    _hierarchy_gate,
    _static_gate,
    _static_release_checks,
    _systemd_gate,
)

ROOT = Path(__file__).resolve().parents[1]


def test_d14_static_release_guards_are_clean():
    checks = _static_release_checks()
    gate = _static_gate(checks)
    assert gate["status"] == "PASS", gate["failures"]
    assert checks["js_hardcoded_ofs"] == []


def test_d14_hierarchy_gate_requires_recent_single_snapshot():
    payload = {
        "health": {"status": "ok", "age_seconds": 60, "last_success_at": "2026-08-28 15:00:00"},
        "snapshot": {
            "rows_total": 2054,
            "distinct_resources": 2054,
            "distinct_last_seen_batches": 1,
        },
    }
    assert _hierarchy_gate(payload)["status"] == "PASS"

    payload["health"]["age_seconds"] = 7201
    assert _hierarchy_gate(payload)["status"] == "FAIL"


def test_d14_hierarchy_gate_treats_healthy_running_sync_as_pending_not_failure():
    payload = {
        "health": {
            "status": "running",
            "age_seconds": 1374,
            "last_success_at": "2026-08-28 15:48:54",
        },
        "snapshot": {
            "rows_total": 2054,
            "distinct_resources": 2054,
            "distinct_last_seen_batches": 1,
        },
    }
    assert _hierarchy_gate(payload)["status"] == "RUNNING_SAFE"


def test_d14_systemd_gate_accepts_active_worker_timer_and_successful_oneshot():
    payload = {
        "worker": {"LoadState": "loaded", "ActiveState": "active", "UnitFileState": "enabled"},
        "hierarchy_timer": {"LoadState": "loaded", "ActiveState": "active", "UnitFileState": "enabled"},
        "hierarchy_service": {"LoadState": "loaded", "Result": "success", "ExecMainStatus": "0"},
    }
    gate = _systemd_gate(payload)
    assert gate["status"] == "PASS"


def test_d14_package_excludes_runtime_secret_and_cache_paths(tmp_path):
    output = (tmp_path / "release.zip").resolve()
    assert _is_excluded(Path(".env"), output) is True
    assert _is_excluded(Path("venv/bin/python"), output) is True
    assert _is_excluded(Path("instance/reports/x.json"), output) is True
    assert _is_excluded(Path("temp_uploads/customer.xlsx"), output) is True
    assert _is_excluded(Path("pkg/__pycache__/x.pyc"), output) is True
    assert _is_excluded(Path("services/module.py"), output) is False


def test_d14_validation_sql_is_read_only_and_checks_hierarchy_health_and_retention():
    text = (ROOT / "database/sql/20260828_ofs_d14_release_validate.sql").read_text(encoding="utf-8")
    upper = text.upper()
    assert "'HIERARCHY'" in upper
    assert "DATE_SUB(CURDATE(), INTERVAL 6 DAY)" in upper
    assert "WORK_DATE > CURDATE()" in upper
    assert "COUNT(DISTINCT LAST_SEEN_AT)" in upper
    assert "INFORMATION_SCHEMA.STATISTICS" in upper
    for forbidden in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "DROP ", "ALTER "):
        assert forbidden not in upper


def test_d14_deploy_guide_preserves_no_routine_full_baseline_and_final_evidence():
    text = (ROOT / "deploy/D14_PRODUCTION_DEPLOY_ROLLBACK.md").read_text(encoding="utf-8")
    assert "Não executar baseline completo por rotina de upgrade" in text
    assert "tools/ofs_d14_release_probe.py" in text
    assert "tools/build_d14_release_package.py" in text
    assert "mode=full" in text
    assert "parent_id" in text
    assert "AUTOMATED_CHECKS_PASS" in text
