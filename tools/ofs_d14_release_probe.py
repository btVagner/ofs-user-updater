from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

HIERARCHY_STALE_SECONDS = 2 * 60 * 60
WORKER_UNIT = "ofs-technician-operational-worker.service"
HIERARCHY_SERVICE_UNIT = "ofs-resource-hierarchy-sync.service"
HIERARCHY_TIMER_UNIT = "ofs-resource-hierarchy-sync.timer"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe consolidado da Demanda 14. Revalida hardening D10-D13, "
            "health estrutural/operacional e, opcionalmente, systemd. "
            "O probe nao executa chamadas Oracle OFS."
        )
    )
    parser.add_argument("--date", help="Data operacional YYYY-MM-DD. Padrao: hoje no timezone central.")
    parser.add_argument("--repetitions", type=int, default=10, help="Repeticoes dos benchmarks locais.")
    parser.add_argument("--no-http", action="store_true", help="Nao mede endpoints Flask via test_client.")
    parser.add_argument("--check-lock", action="store_true", help="Verifica GET_LOCK do worker.")
    parser.add_argument(
        "--systemd",
        action="store_true",
        help="Consulta runtime systemd do worker e do hierarchy service/timer.",
    )
    parser.add_argument(
        "--static-only",
        action="store_true",
        help="Executa apenas os guards estaticos da D14; nao acessa MySQL/Flask/systemd.",
    )
    parser.add_argument("--output", help="Arquivo JSON opcional para salvar o relatorio.")
    return parser.parse_args()


def _safe_component(name: str, fn: Callable[[], Any], errors: List[dict]) -> Any:
    try:
        return fn()
    except Exception as exc:
        errors.append({"component": name, "error": _safe_error(exc)})
        return {"error": _safe_error(exc)}


def _safe_error(exc: Any) -> str:
    text = str(exc or "").strip() or exc.__class__.__name__
    text = re.sub(r"https?://\S+", "<url>", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?i)\b(password|passwd|token|access_token|refresh_token|client_secret|secret|authorization|assertion)\b\s*[:=]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )
    return text[:500]


def _read(relative: str) -> str:
    return (ROOT_DIR / relative).read_text(encoding="utf-8")


def _js_hardcoded_ofs() -> List[str]:
    matches: List[str] = []
    for path in sorted((ROOT_DIR / "static").rglob("*.js")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if re.search(r"(?:['\"`])/ofs(?:/|['\"`])", line):
                matches.append(f"{path.relative_to(ROOT_DIR)}:{line_no}")
    return matches


def _static_release_checks() -> dict:
    operational = _read("services/ofs_technician_operational_service.py")
    alert = _read("services/ofs_technician_alert_service.py")
    monitor = _read("services/ofs_technician_monitor_service.py")
    routes = _read("routes/ofs_technician_monitor_routes.py")
    js = _read("static/js/dashboard_technicians.js")
    hierarchy = _read("services/ofs_resource_hierarchy_service.py")
    hierarchy_tool = _read("tools/sync_ofs_resource_hierarchy.py")
    worker_unit = _read("deploy/systemd/ofs-technician-operational-worker.service.example")
    hierarchy_service = _read("deploy/systemd/ofs-resource-hierarchy-sync.service.example")
    hierarchy_timer = _read("deploy/systemd/ofs-resource-hierarchy-sync.timer.example")
    gitignore = _read(".gitignore")

    customer_home_guard = (
        'CUSTOMER_HOME_ACTIVITY_CATEGORY = "customer_home"' in operational
        and "is_active" in operational
        and "category" in operational
    )
    extra_working_guard = (
        "extra_working" in alert and "extra-working" in alert and "extraworking" in alert
    )
    activation_formula_guard = bool(
        re.search(r"activation_percent\s*=.*working_active_route_count\s*/\s*working_count", monitor)
    )
    hierarchy_version_guard = all(
        token in js
        for token in (
            "hierarchyVersionObserved",
            "hierarchyCacheInvalidations",
            "state.childCache.clear()",
            "state.expanded.clear()",
        )
    ) and '"version": last_success_at' in monitor

    source_bundle = "\n".join((hierarchy, hierarchy_tool, hierarchy_service, hierarchy_timer)).lower()
    hierarchy_no_baseline = (
        "baseline-once" not in source_bundle
        and "ofs_technician_operational_worker.py" not in source_bundle
        and ".get_route(" not in source_bundle
    )

    return {
        "customer_home_guard_present": customer_home_guard,
        "extra_working_guard_present": extra_working_guard,
        "activation_formula_working_denominator": activation_formula_guard,
        "alert_codes_translated_in_ui": all(code in js for code in (
            "ROUTE_NOT_STARTED",
            "ROUTE_ACTIVE_AFTER_SHIFT",
            "ACTIVITY_OPEN_AFTER_SHIFT",
        )) and "Alerta operacional" in js,
        "global_freshness_not_in_technician_block": (
            "hierarchy_sync" not in js[js.index("function appendTechnicianDetails"):js.index("function appendAggregateDetails")]
            and "health.sources" not in js[js.index("function appendTechnicianDetails"):js.index("function appendAggregateDetails")]
        ),
        "hierarchy_health_is_global": (
            "HIERARCHY_HEALTH_SOURCE = \"hierarchy\"" in monitor
            and "RUNTIME_HEALTH_SOURCES = (SOURCE_EVENTS, SOURCE_ACTIVITIES, SOURCE_CALENDARS)" in monitor
        ),
        "hierarchy_cache_version_invalidation": hierarchy_version_guard,
        "kpi_filter_server_side": (
            "data-technicians-kpi-filter" in js
            and "filter: state.serverFilter" in js
            and "only_problems: state.onlyProblems ? 1 : null" in js
            and "filter_name" in routes
        ),
        "ui_mode_full_absent": 'mode: "full"' not in js and "mode=full" not in js,
        "ui_parent_id_present": "parent_id: parentId" in js,
        "caught_up_null_not_coerced": (
            "const caughtUp = health.events_caught_up;" in js
            and "!!health.events_caught_up" not in js
        ),
        "monitor_request_path_has_no_ofs_client": "OFSClient" not in monitor and "requests." not in monitor,
        "hierarchy_sync_has_no_baseline_or_get_route": hierarchy_no_baseline,
        "hierarchy_failure_preserves_snapshot_pattern": (
            "snapshot = fetch_resource_hierarchy_snapshot" in hierarchy
            and "repository.replace_snapshot" in hierarchy
            and "except Exception as exc" in hierarchy
        ),
        "worker_timeout_stop_300": "TimeoutStopSec=300" in worker_unit,
        "worker_graceful_sigint": "KillSignal=SIGINT" in worker_unit,
        "hierarchy_timer_hourly_persistent": (
            "OnUnitActiveSec=1h" in hierarchy_timer
            and "Persistent=true" in hierarchy_timer
            and "Unit=ofs-resource-hierarchy-sync.service" in hierarchy_timer
        ),
        "hierarchy_service_oneshot": (
            "Type=oneshot" in hierarchy_service
            and "tools/sync_ofs_resource_hierarchy.py" in hierarchy_service
            and "TimeoutStartSec=180" in hierarchy_service
        ),
        "js_hardcoded_ofs": _js_hardcoded_ofs(),
        "gitignore_runtime_paths": (
            any(line.strip() == "instance/" for line in gitignore.splitlines())
            and any(line.strip() == "temp_uploads/" for line in gitignore.splitlines())
            and any(line.strip() == "venv/" for line in gitignore.splitlines())
            and any(line.strip() == ".venv/" for line in gitignore.splitlines())
            and any(line.strip() == ".pytest_cache/" for line in gitignore.splitlines())
            and any(line.strip() == "*.py[cod]" for line in gitignore.splitlines())
            and any(line.strip() == ".env" for line in gitignore.splitlines())
        ),
    }


def _collect_hierarchy_health() -> dict:
    from database.connection import get_connection
    from ofs.config import get_ofs_root_resource_id

    root_resource_id = get_ofs_root_resource_id()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT source_name,status,last_started_at,last_success_at,last_finished_at,
                   TIMESTAMPDIFF(SECOND,last_success_at,UTC_TIMESTAMP(6)) AS age_seconds,
                   error_code,updated_at
            FROM ofs_operational_sync_state
            WHERE source_name='hierarchy'
            LIMIT 1
            """
        )
        health = dict(cur.fetchone() or {})
        cur.execute(
            """
            SELECT COUNT(*) AS rows_total,
                   COUNT(DISTINCT resource_id) AS distinct_resources,
                   COUNT(DISTINCT last_seen_at) AS distinct_last_seen_batches,
                   MIN(last_seen_at) AS min_last_seen_at,
                   MAX(last_seen_at) AS max_last_seen_at,
                   MAX(depth) AS max_depth
            FROM ofs_resource_hierarchy
            WHERE root_resource_id=%s
            """,
            (root_resource_id,),
        )
        snapshot = dict(cur.fetchone() or {})
        return {"root_resource_id": root_resource_id, "health": health, "snapshot": snapshot}
    finally:
        cur.close()
        conn.close()


def _hierarchy_gate(payload: Mapping[str, Any]) -> dict:
    if not payload or payload.get("error"):
        return {"status": "INCOMPLETE", "reason": "health estrutural indisponivel"}
    health = payload.get("health") or {}
    snapshot = payload.get("snapshot") or {}
    status = str(health.get("status") or "").lower()
    age = health.get("age_seconds")
    rows_total = int(snapshot.get("rows_total") or 0)
    distinct = int(snapshot.get("distinct_resources") or 0)
    batches = int(snapshot.get("distinct_last_seen_batches") or 0)

    snapshot_ok = rows_total > 0 and rows_total == distinct and batches <= 1
    last_success_recent = (
        age is not None
        and 0 <= float(age) <= HIERARCHY_STALE_SECONDS
        and health.get("last_success_at") is not None
    )

    if status == "ok" and snapshot_ok and last_success_recent:
        gate_status = "PASS"
    elif status == "running" and snapshot_ok and last_success_recent:
        # Uma sincronizacao em andamento e um estado operacional esperado. O snapshot
        # anterior continua valido por contrato D04/D13, mas a execucao corrente ainda
        # precisa terminar antes do GO final de release. Portanto isto e evidencia
        # pendente, nao um sinal de NO-GO.
        gate_status = "RUNNING_SAFE"
    else:
        gate_status = "FAIL"

    return {
        "status": gate_status,
        "source_status": status or None,
        "age_seconds": age,
        "threshold_seconds": HIERARCHY_STALE_SECONDS,
        "rows_total": rows_total,
        "distinct_resources": distinct,
        "distinct_last_seen_batches": batches,
        "last_success_at": health.get("last_success_at"),
    }


def _systemctl_show(unit: str) -> dict:
    properties = (
        "LoadState", "ActiveState", "SubState", "UnitFileState", "Result", "ExecMainStatus",
        "ActiveEnterTimestamp", "InactiveEnterTimestamp", "NextElapseUSecRealtime",
    )
    command = ["systemctl", "show", unit]
    for property_name in properties:
        command.extend(["--property", property_name])
    proc = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"systemctl show falhou para {unit}")
    result: Dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key] = value
    return result


def _collect_systemd() -> dict:
    return {
        "worker": {"unit": WORKER_UNIT, **_systemctl_show(WORKER_UNIT)},
        "hierarchy_service": {"unit": HIERARCHY_SERVICE_UNIT, **_systemctl_show(HIERARCHY_SERVICE_UNIT)},
        "hierarchy_timer": {"unit": HIERARCHY_TIMER_UNIT, **_systemctl_show(HIERARCHY_TIMER_UNIT)},
    }


def _unit_enabled(item: Mapping[str, Any]) -> bool:
    return str(item.get("UnitFileState") or "").lower() in {"enabled", "enabled-runtime", "static"}


def _systemd_gate(payload: Mapping[str, Any]) -> dict:
    if not payload or payload.get("error"):
        return {"status": "INCOMPLETE", "reason": "runtime systemd indisponivel"}
    worker = payload.get("worker") or {}
    service = payload.get("hierarchy_service") or {}
    timer = payload.get("hierarchy_timer") or {}

    worker_ok = (
        str(worker.get("LoadState") or "").lower() == "loaded"
        and str(worker.get("ActiveState") or "").lower() == "active"
        and _unit_enabled(worker)
    )
    timer_ok = (
        str(timer.get("LoadState") or "").lower() == "loaded"
        and str(timer.get("ActiveState") or "").lower() == "active"
        and _unit_enabled(timer)
    )
    service_result = str(service.get("Result") or "").lower()
    service_ok = (
        str(service.get("LoadState") or "").lower() == "loaded"
        and service_result in {"success", ""}
        and str(service.get("ExecMainStatus") or "0") in {"0", ""}
    )
    return {
        "status": "PASS" if worker_ok and timer_ok and service_ok else "FAIL",
        "worker_ok": worker_ok,
        "hierarchy_timer_ok": timer_ok,
        "hierarchy_service_last_result_ok": service_ok,
    }


def _static_gate(checks: Mapping[str, Any]) -> dict:
    failures: List[str] = []
    for key, value in checks.items():
        if key == "js_hardcoded_ofs":
            if value:
                failures.append(f"JavaScript contem /ofs hardcoded: {value}")
            continue
        if value is not True:
            failures.append(key)
    return {"status": "PASS" if not failures else "FAIL", "failures": failures}


def _release_gate(report: Mapping[str, Any], *, systemd_required: bool, static_only: bool) -> dict:
    failures: List[str] = []
    incomplete: List[str] = []

    static_gate = report.get("static_gate") or {}
    if static_gate.get("status") != "PASS":
        failures.extend([f"static:{item}" for item in static_gate.get("failures") or ["guard estatico falhou"]])

    if static_only:
        return {
            "status": "STATIC_CHECKS_PASS" if not failures else "NO_GO_SIGNAL",
            "failures": failures,
            "incomplete": [],
        }

    d10_gate = report.get("d10_automatic_gate") or {}
    if d10_gate.get("status") == "NO_GO_SIGNAL":
        failures.extend([f"d10:{item}" for item in d10_gate.get("failures") or []])
    elif d10_gate.get("status") != "AUTOMATED_CHECKS_PASS":
        incomplete.extend([f"d10:{item}" for item in d10_gate.get("incomplete") or ["evidencia D10 incompleta"]])

    hierarchy_gate = report.get("hierarchy_gate") or {}
    if hierarchy_gate.get("status") == "FAIL":
        failures.append("hierarchy health/snapshot nao passou o gate")
    elif hierarchy_gate.get("status") == "RUNNING_SAFE":
        incomplete.append("hierarchy sync em andamento; aguardar conclusao bem-sucedida para o GO final")
    elif hierarchy_gate.get("status") != "PASS":
        incomplete.append("hierarchy health nao pode ser comprovado")

    if systemd_required:
        systemd_gate = report.get("systemd_gate") or {}
        if systemd_gate.get("status") == "FAIL":
            failures.append("worker/timer/service systemd nao estao simultaneamente saudaveis")
        elif systemd_gate.get("status") != "PASS":
            incomplete.append("runtime systemd nao pode ser comprovado")
    else:
        incomplete.append("runtime systemd nao foi solicitado; execute novamente com --systemd em producao")

    if failures:
        status = "NO_GO_SIGNAL"
    elif incomplete:
        status = "PENDING_EVIDENCE"
    else:
        status = "AUTOMATED_CHECKS_PASS"
    return {"status": status, "failures": failures, "incomplete": incomplete}


def _collect_d10(args: argparse.Namespace, work_date: date, errors: List[dict]) -> dict:
    from tools import ofs_d10_hardening_probe as d10

    repetitions = max(int(args.repetitions), 1)
    report: Dict[str, Any] = {
        "static": _safe_component("d10_static", d10._static_checks, errors),
        "mysql": _safe_component("d10_mysql", lambda: d10._collect_mysql_hardening(work_date), errors),
        "dashboard": _safe_component(
            "d10_dashboard", lambda: d10._collect_dashboard(repetitions, not args.no_http), errors
        ),
        "monitor": _safe_component(
            "d10_monitor", lambda: d10._collect_monitor(work_date, repetitions, not args.no_http), errors
        ),
    }
    if args.check_lock:
        report["lock"] = _safe_component("d10_lock", d10._check_worker_lock, errors)
    report["health_gate"] = d10._health_gate(report.get("monitor") or {})
    d10_input = {**report, "errors": [item for item in errors if item["component"].startswith("d10_")]}
    report["automatic_gate"] = d10._automatic_gate(d10_input)
    return report


def main() -> int:
    args = _parse_args()
    errors: List[dict] = []
    static_checks = _static_release_checks()
    report: Dict[str, Any] = {
        "probe": "ofs_d14_release_probe",
        "scope": "final_release_hardening_no_direct_oracle_ofs_calls",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "static": static_checks,
        "static_gate": _static_gate(static_checks),
    }

    if not args.static_only:
        from services.ofs_technician_monitor_service import default_monitor_work_date

        work_date = date.fromisoformat(args.date) if args.date else default_monitor_work_date()
        report["work_date"] = work_date.isoformat()
        d10 = _collect_d10(args, work_date, errors)
        report["d10"] = d10
        report["d10_automatic_gate"] = d10.get("automatic_gate")
        report["hierarchy"] = _safe_component("hierarchy", _collect_hierarchy_health, errors)
        report["hierarchy_gate"] = _hierarchy_gate(report.get("hierarchy") or {})
        if args.systemd:
            report["systemd"] = _safe_component("systemd", _collect_systemd, errors)
            report["systemd_gate"] = _systemd_gate(report.get("systemd") or {})

    report["errors"] = errors
    report["release_gate"] = _release_gate(
        report,
        systemd_required=bool(args.systemd),
        static_only=bool(args.static_only),
    )

    rendered = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    print(rendered)
    if args.output:
        output = Path(args.output)
        if not output.is_absolute():
            output = ROOT_DIR / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OFS_D14_RELEASE] relatorio salvo em: {output}", file=sys.stderr)

    return 2 if report["release_gate"]["status"] == "NO_GO_SIGNAL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
