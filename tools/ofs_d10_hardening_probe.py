from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from ofs.config import get_ofs_root_resource_id  # noqa: E402
from services.ofs_technician_monitor_service import (  # noqa: E402
    MySQLTechnicianMonitorRepository,
    TechnicianMonitorService,
    default_monitor_work_date,
)
from services.ofs_technician_operational_service import (  # noqa: E402
    LOCK_NAME,
    sanitize_operational_error,
)

RUNTIME_HEALTH_SOURCES = ("events", "activities", "calendars")
HEALTH_THRESHOLDS_SECONDS = {
    "events": 180,
    "activities": 1800,
    "calendars": 5400,
    "routes": None,
}
HARDENING_TABLES = (
    "ofs_resource_hierarchy",
    "ofs_technician_operational_state",
    "ofs_activity_operational_state",
    "ofs_event_cursor",
    "ofs_operational_sync_state",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe consolidado da Demanda 10. Mede somente Flask/MySQL/local; "
            "não consulta Oracle OFS."
        )
    )
    parser.add_argument("--date", help="Data operacional YYYY-MM-DD. Padrão: hoje no timezone central.")
    parser.add_argument("--repetitions", type=int, default=10, help="Repetições para benchmarks locais.")
    parser.add_argument(
        "--no-http",
        action="store_true",
        help="Não mede endpoints Flask via test_client; mantém apenas serviço/MySQL.",
    )
    parser.add_argument(
        "--check-lock",
        action="store_true",
        help="Verifica se o GET_LOCK do worker está ocupado sem mantê-lo adquirido.",
    )
    parser.add_argument("--output", help="Caminho opcional para salvar o JSON do relatório.")
    return parser.parse_args()


def _safe_component(name: str, fn: Callable[[], Any], errors: List[dict]) -> Any:
    try:
        return fn()
    except Exception as exc:  # hardening probe must preserve partial evidence
        message = sanitize_operational_error(exc)
        errors.append({"component": name, "error": message})
        return {"error": message}


def _rows_as_dicts(cur) -> List[dict]:
    return [dict(row) for row in (cur.fetchall() or [])]


def _group_indexes(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, dict]]:
    grouped: Dict[str, Dict[str, dict]] = {}
    for row in rows:
        table = str(row.get("TABLE_NAME") or "")
        index = str(row.get("INDEX_NAME") or "")
        if not table or not index:
            continue
        table_indexes = grouped.setdefault(table, {})
        item = table_indexes.setdefault(
            index,
            {
                "unique": not bool(row.get("NON_UNIQUE")),
                "columns": [],
                "cardinality": row.get("CARDINALITY"),
            },
        )
        item["columns"].append(str(row.get("COLUMN_NAME") or ""))
    return grouped


def _collect_mysql_hardening(work_date: date) -> dict:
    from database.connection import get_connection

    cutoff = work_date - timedelta(days=6)
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        retention = {}
        for key, table in (
            ("technicians", "ofs_technician_operational_state"),
            ("activities", "ofs_activity_operational_state"),
        ):
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS rows_total,
                    COUNT(DISTINCT work_date) AS dates_total,
                    MIN(work_date) AS min_work_date,
                    MAX(work_date) AS max_work_date,
                    SUM(work_date < %s) AS expired_rows,
                    SUM(work_date > %s) AS future_rows
                FROM {table}
                """,
                (cutoff, work_date),
            )
            retention[key] = dict(cur.fetchone() or {})

            cur.execute(
                f"SELECT work_date, COUNT(*) AS rows_total FROM {table} GROUP BY work_date ORDER BY work_date"
            )
            retention[key]["rows_by_date"] = _rows_as_dicts(cur)

        root_resource_id = get_ofs_root_resource_id()
        cur.execute(
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(DISTINCT resource_id) AS distinct_resources,
                MAX(depth) AS max_depth,
                MIN(last_seen_at) AS oldest_last_seen_at,
                MAX(last_seen_at) AS newest_last_seen_at
            FROM ofs_resource_hierarchy
            WHERE root_resource_id=%s
            """,
            (root_resource_id,),
        )
        hierarchy = {"root_resource_id": root_resource_id, **dict(cur.fetchone() or {})}

        cur.execute(
            """
            SELECT
                source_name,
                status,
                last_started_at,
                last_success_at,
                last_finished_at,
                TIMESTAMPDIFF(SECOND, last_success_at, UTC_TIMESTAMP(6)) AS age_seconds,
                error_code,
                updated_at
            FROM ofs_operational_sync_state
            WHERE source_name IN ('events','activities','calendars','routes')
            ORDER BY source_name
            """
        )
        health_rows = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT
                cursor_key,
                (subscription_id IS NOT NULL AND subscription_id <> '') AS has_subscription,
                (next_page IS NOT NULL AND next_page <> '') AS has_next_page,
                CHAR_LENGTH(next_page) AS next_page_length,
                subscription_created_at,
                baseline_completed_at,
                last_poll_success_at,
                last_event_at,
                last_error_code,
                updated_at
            FROM ofs_event_cursor
            ORDER BY cursor_key
            """
        )
        cursor_rows = _rows_as_dicts(cur)

        placeholders = ",".join(["%s"] * len(HARDENING_TABLES))
        cur.execute(
            f"""
            SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH,
                   DATA_LENGTH + INDEX_LENGTH AS total_bytes
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE()
              AND TABLE_NAME IN ({placeholders})
            ORDER BY TABLE_NAME
            """,
            HARDENING_TABLES,
        )
        table_sizes_rows = _rows_as_dicts(cur)
        table_sizes = {
            str(row["TABLE_NAME"]): {
                "estimated_rows": int(row.get("TABLE_ROWS") or 0),
                "data_bytes": int(row.get("DATA_LENGTH") or 0),
                "index_bytes": int(row.get("INDEX_LENGTH") or 0),
                "total_bytes": int(row.get("total_bytes") or 0),
            }
            for row in table_sizes_rows
        }

        cur.execute(
            f"""
            SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, CARDINALITY
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA=DATABASE()
              AND TABLE_NAME IN ({placeholders})
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
            """,
            HARDENING_TABLES,
        )
        indexes = _group_indexes(_rows_as_dicts(cur))

        estimate = _estimate_seven_day_storage(retention, table_sizes)

        return {
            "retention_window": {
                "cutoff": cutoff.isoformat(),
                "today": work_date.isoformat(),
                "retention_days": 7,
                "datasets": retention,
            },
            "hierarchy": hierarchy,
            "health_rows": health_rows,
            "cursor": cursor_rows,
            "table_sizes": table_sizes,
            "indexes": indexes,
            "storage_estimate_7d": estimate,
        }
    finally:
        cur.close()
        conn.close()


def _estimate_seven_day_storage(retention: Mapping[str, Any], table_sizes: Mapping[str, Any]) -> dict:
    fixed_tables = ("ofs_resource_hierarchy", "ofs_event_cursor", "ofs_operational_sync_state")
    variable = (
        ("technicians", "ofs_technician_operational_state"),
        ("activities", "ofs_activity_operational_state"),
    )

    fixed_bytes = sum(int((table_sizes.get(name) or {}).get("total_bytes") or 0) for name in fixed_tables)
    estimated_variable = 0.0
    detail = {}
    for dataset, table in variable:
        current_bytes = int((table_sizes.get(table) or {}).get("total_bytes") or 0)
        distinct_dates = int(((retention.get(dataset) or {}).get("dates_total") or 0))
        effective_days = max(distinct_dates, 1)
        projected = (current_bytes / effective_days) * 7
        detail[table] = {
            "current_bytes": current_bytes,
            "current_distinct_dates": distinct_dates,
            "projected_7d_bytes": round(projected),
        }
        estimated_variable += projected

    return {
        "method": "aproximacao_linear_por_dias_materializados_usando_DATA_LENGTH+INDEX_LENGTH",
        "note": "InnoDB reporta alocacao aproximada; usar como ordem de grandeza, nao capacidade exata.",
        "fixed_tables_bytes": fixed_bytes,
        "date_scoped_tables": detail,
        "projected_total_7d_bytes": round(fixed_bytes + estimated_variable),
    }


def _check_worker_lock() -> dict:
    from database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()
    acquired = False
    try:
        cur.execute("SELECT GET_LOCK(%s,0)", (LOCK_NAME,))
        row = cur.fetchone()
        acquired = bool(row and row[0] == 1)
        return {
            "lock_name": LOCK_NAME,
            "available": acquired,
            "worker_likely_holds_lock": not acquired,
        }
    finally:
        try:
            if acquired:
                cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
                cur.fetchone()
        finally:
            cur.close()
            conn.close()


def _static_checks() -> dict:
    technicians_js_path = ROOT_DIR / "static/js/dashboard_technicians.js"
    technicians_js = technicians_js_path.read_text(encoding="utf-8")
    dashboard_template = (ROOT_DIR / "templates/dashboard_operacional.html").read_text(encoding="utf-8")
    home_routes = (ROOT_DIR / "routes/home_routes.py").read_text(encoding="utf-8")
    gitignore = (ROOT_DIR / ".gitignore").read_text(encoding="utf-8")

    hardcoded_ofs = []
    for path in sorted((ROOT_DIR / "static").rglob("*.js")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if re.search(r"(?:['\"`])/ofs(?:/|['\"`])", line):
                hardcoded_ofs.append(f"{path.relative_to(ROOT_DIR)}:{line_no}")

    worker_source = (ROOT_DIR / "services/ofs_technician_operational_service.py").read_text(encoding="utf-8")
    run_forever_match = re.search(
        r"def run_forever\(.*?(?=\n    def |\Z)", worker_source, flags=re.DOTALL
    )
    run_forever_source = run_forever_match.group(0) if run_forever_match else ""

    return {
        "js_hardcoded_ofs": hardcoded_ofs,
        "dashboard_browser_source_has_dashboard_rows": (
            "dashboard_rows" in technicians_js or "dashboard_rows" in dashboard_template
        ),
        "status_route_uses_metadata_only_reader": "get_dashboard_snapshot_status" in home_routes,
        "technicians_ui_uses_mode_full": "mode=full" in technicians_js or 'mode: "full"' in technicians_js,
        "technicians_ui_uses_parent_id": "parent_id" in technicians_js,
        "technicians_polling_checks_document_hidden": "document.hidden" in technicians_js,
        "run_forever_calls_get_route_directly": ".get_route(" in run_forever_source,
        "gitignore_instance": any(line.strip() == "instance/" for line in gitignore.splitlines()),
        "gitignore_temp_uploads": any(line.strip() == "temp_uploads/" for line in gitignore.splitlines()),
        "runtime_artifact_counts": {
            "instance_files": _count_files(ROOT_DIR / "instance"),
            "temp_upload_files": _count_files(ROOT_DIR / "temp_uploads"),
        },
    }


def _count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*") if item.is_file())


def _health_gate(monitor_report: Mapping[str, Any]) -> dict:
    direct = monitor_report.get("direct") if isinstance(monitor_report, Mapping) else None
    health = (direct or {}).get("health") if isinstance(direct, Mapping) else None
    if not isinstance(health, Mapping):
        return {"status": "INCOMPLETE", "reason": "health do monitor indisponivel"}

    sources = health.get("sources") or {}
    checks = {}
    runtime_ok = True
    for source in RUNTIME_HEALTH_SOURCES:
        item = sources.get(source) or {}
        state = item.get("state")
        age = item.get("age_seconds")
        threshold = item.get("threshold_seconds")
        state_ok = str(state or "").upper() == "OK"
        ok = state_ok and age is not None and threshold is not None and float(age) <= float(threshold)
        checks[source] = {
            "ok": ok,
            "state": state,
            "age_seconds": age,
            "threshold_seconds": threshold,
        }
        runtime_ok = runtime_ok and ok

    return {
        "status": "PASS" if runtime_ok and health.get("overall_integrity") == "ok" else "FAIL",
        "overall_integrity": health.get("overall_integrity"),
        "events_caught_up": health.get("events_caught_up"),
        "sources": checks,
    }


def _automatic_gate(report: Mapping[str, Any]) -> dict:
    failures: List[str] = []
    incomplete: List[str] = []

    if report.get("errors"):
        incomplete.append("um ou mais componentes do probe falharam")

    static = report.get("static") or {}
    if static.get("js_hardcoded_ofs"):
        failures.append("JavaScript contem /ofs hardcoded")
    if static.get("dashboard_browser_source_has_dashboard_rows"):
        failures.append("dashboard_rows reapareceu em fonte entregue ao browser")
    if not static.get("status_route_uses_metadata_only_reader"):
        failures.append("/dashboard/status nao esta claramente metadata-only")
    if static.get("technicians_ui_uses_mode_full"):
        failures.append("UI de tecnicos usa mode=full")
    if not static.get("technicians_ui_uses_parent_id"):
        failures.append("UI de tecnicos nao evidencia expansao por parent_id")
    if not static.get("technicians_polling_checks_document_hidden"):
        failures.append("polling de tecnicos nao respeita document.hidden")
    if static.get("run_forever_calls_get_route_directly"):
        failures.append("run_forever chama GET Route diretamente fora do baseline")
    if not static.get("gitignore_instance") or not static.get("gitignore_temp_uploads"):
        failures.append("paths de runtime nao estao totalmente protegidos no .gitignore")

    mysql = report.get("mysql") or {}
    retention = ((mysql.get("retention_window") or {}).get("datasets") or {}) if isinstance(mysql, Mapping) else {}
    for dataset in ("technicians", "activities"):
        row = retention.get(dataset) or {}
        if int(row.get("expired_rows") or 0) > 0:
            failures.append(f"{dataset}: ha registros expirados fora da janela de 7 dias")
        if int(row.get("future_rows") or 0) > 0:
            failures.append(f"{dataset}: ha materializacao futura")

    dashboard = report.get("dashboard") or {}
    if isinstance(dashboard, Mapping) and "error" not in dashboard:
        snapshot = dashboard.get("snapshot") or {}
        render = dashboard.get("render") or {}
        if int(snapshot.get("dashboard_rows_db") or 0) != 0:
            failures.append("snapshot atual contem dashboard_rows bruto")
        if int(render.get("browser_dashboard_rows") or 0) != 0:
            failures.append("browser recebe dashboard_rows bruto")
    else:
        incomplete.append("benchmark do Dashboard OS indisponivel")

    health_gate = report.get("health_gate") or {}
    if health_gate.get("status") == "FAIL":
        failures.append("Events/Activities/Calendars nao estao simultaneamente dentro dos thresholds")
    elif health_gate.get("status") != "PASS":
        incomplete.append("health/freshness nao pode ser comprovado")

    if failures:
        status = "NO_GO_SIGNAL"
    elif incomplete:
        status = "PENDING_EVIDENCE"
    else:
        status = "AUTOMATED_CHECKS_PASS"

    return {"status": status, "failures": failures, "incomplete": incomplete}


def _collect_dashboard(repetitions: int, include_http: bool) -> dict:
    from tools.dashboard_perf_probe import measure_render, measure_snapshot, measure_status_endpoint

    result = {
        "snapshot": measure_snapshot(repetitions),
        "render": measure_render(),
    }
    if include_http:
        result["status_endpoint"] = measure_status_endpoint(repetitions)
    return result


def _collect_monitor(work_date: date, repetitions: int, include_http: bool) -> dict:
    from tools.ofs_technician_monitor_probe import _collect_direct, _collect_http

    repository = MySQLTechnicianMonitorRepository()
    service = TechnicianMonitorService(repository=repository)
    result = {
        "direct": _collect_direct(service, work_date, repetitions),
        "explain": repository.explain_queries(work_date),
    }
    if include_http:
        result["http"] = _collect_http(work_date, repetitions)
    return result


def main() -> int:
    args = _parse_args()
    repetitions = max(int(args.repetitions), 1)
    work_date = date.fromisoformat(args.date) if args.date else default_monitor_work_date()
    errors: List[dict] = []

    report: Dict[str, Any] = {
        "probe": "ofs_d10_hardening_probe",
        "scope": "local_mysql_flask_only_no_oracle_ofs_calls",
        "work_date": work_date.isoformat(),
        "repetitions": repetitions,
        "thresholds_seconds": HEALTH_THRESHOLDS_SECONDS,
        "static": _safe_component("static", _static_checks, errors),
        "mysql": _safe_component("mysql", lambda: _collect_mysql_hardening(work_date), errors),
        "dashboard": _safe_component(
            "dashboard", lambda: _collect_dashboard(repetitions, not args.no_http), errors
        ),
        "monitor": _safe_component(
            "monitor", lambda: _collect_monitor(work_date, repetitions, not args.no_http), errors
        ),
    }
    if args.check_lock:
        report["lock"] = _safe_component("lock", _check_worker_lock, errors)

    report["errors"] = errors
    report["health_gate"] = _health_gate(report.get("monitor") or {})
    report["automatic_gate"] = _automatic_gate(report)

    rendered = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    print(rendered)

    if args.output:
        output = Path(args.output)
        if not output.is_absolute():
            output = ROOT_DIR / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OFS_D10_HARDENING] relatorio salvo em: {output}", file=sys.stderr)

    return 0 if report["automatic_gate"]["status"] != "NO_GO_SIGNAL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
