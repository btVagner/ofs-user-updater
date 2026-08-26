#!/usr/bin/env python3
"""Baseline seguro de performance do Dashboard Operacional.

A ferramenta não imprime payloads, atividades, credenciais, tokens ou conteúdo
funcional. Por padrão mede apenas o snapshot existente. O modo --live-ofs faz
consultas de leitura ao OFS para medir a atualização, mas não persiste um novo
snapshot e neutraliza as atualizações de progresso durante a coleta.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SENSITIVE_MARKERS = ("PASSWORD", "PASS", "TOKEN", "SECRET", "PRIVATE", "API_KEY", "AUTH")


def _redact(text: Any) -> str:
    value = str(text or "")
    for key, secret in os.environ.items():
        upper_key = key.upper()
        if secret and any(marker in upper_key for marker in SENSITIVE_MARKERS):
            value = value.replace(secret, "[REDACTED]")
    return value


def _bytes_text(value: int | float | None) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    units = ("B", "KiB", "MiB", "GiB")
    index = 0
    while number >= 1024 and index < len(units) - 1:
        number /= 1024
        index += 1
    return f"{number:.2f} {units[index]}"


def _print_metric(name: str, value: Any, unit: str = "") -> None:
    suffix = f" {unit}" if unit else ""
    print(f"{name}: {value}{suffix}")


def _load_service():
    from services import dashboard_operacional_service as service

    return service


def measure_snapshot(repetitions: int) -> dict[str, Any]:
    from database.connection import get_connection

    service = _load_service()
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                status,
                OCTET_LENGTH(CAST(payload_json AS CHAR)) AS payload_text_bytes,
                JSON_STORAGE_SIZE(payload_json) AS payload_storage_bytes,
                COALESCE(JSON_LENGTH(JSON_EXTRACT(payload_json, '$.dashboard_rows')), 0) AS dashboard_rows,
                COALESCE(JSON_LENGTH(JSON_EXTRACT(payload_json, '$.filter_read_model.types')), 0) AS filter_types,
                updated_at,
                expires_at,
                started_at,
                finished_at,
                CASE
                    WHEN started_at IS NOT NULL AND finished_at IS NOT NULL
                    THEN TIMESTAMPDIFF(MICROSECOND, started_at, finished_at) / 1000
                    ELSE NULL
                END AS last_generation_ms
            FROM dashboard_operacional_snapshot
            WHERE snapshot_key = %s
            """,
            (service.SNAPSHOT_KEY,),
        )
        db_metrics = cur.fetchone() or {}
    finally:
        cur.close()
        conn.close()

    timings_ms: list[float] = []
    snapshot = None
    for _ in range(max(1, repetitions)):
        start = time.perf_counter()
        snapshot = service._load_snapshot()
        timings_ms.append((time.perf_counter() - start) * 1000)

    payload = (snapshot or {}).get("payload") or {}
    serialized_payload = json.dumps(payload, ensure_ascii=False)
    dashboard_rows = payload.get("dashboard_rows") if isinstance(payload, dict) else []
    filter_types = (
        ((payload.get("filter_read_model") or {}).get("types") or {})
        if isinstance(payload, dict)
        else {}
    )

    return {
        "status": db_metrics.get("status") or (snapshot or {}).get("status"),
        "payload_text_bytes_db": db_metrics.get("payload_text_bytes"),
        "payload_storage_bytes_db": db_metrics.get("payload_storage_bytes"),
        "payload_serialized_bytes_python": len(serialized_payload.encode("utf-8")),
        "dashboard_rows_db": db_metrics.get("dashboard_rows"),
        "dashboard_rows_python": len(dashboard_rows) if isinstance(dashboard_rows, list) else 0,
        "filter_types_db": db_metrics.get("filter_types"),
        "filter_types_python": len(filter_types) if isinstance(filter_types, dict) else 0,
        "snapshot_read_ms_avg": statistics.mean(timings_ms),
        "snapshot_read_ms_min": min(timings_ms),
        "snapshot_read_ms_max": max(timings_ms),
        "updated_at": db_metrics.get("updated_at"),
        "expires_at": db_metrics.get("expires_at"),
        "last_generation_ms": db_metrics.get("last_generation_ms"),
    }


def measure_render() -> dict[str, Any]:
    # Reutiliza a aplicação/rota real, mas congela o snapshot já lido para impedir
    # que a medição dispare refresh em background caso ele esteja expirado.
    import app as app_module
    from routes import home_routes

    app = app_module.app
    service = _load_service()
    snapshot_row = service._load_snapshot()
    serialized_snapshot = service._serialize_snapshot(snapshot_row)
    payload = serialized_snapshot.get("payload") or {}
    dashboard_rows = payload.get("dashboard_rows") if isinstance(payload, dict) else []
    filter_types = (
        ((payload.get("filter_read_model") or {}).get("types") or {})
        if isinstance(payload, dict)
        else {}
    )

    with app.test_request_context("/"):
        start_json = time.perf_counter()
        embedded_json = app.jinja_env.from_string("{{ payload|tojson }}").render(payload=payload)
        json_ms = (time.perf_counter() - start_json) * 1000

    original_snapshot_getter = home_routes.get_or_start_dashboard_snapshot
    original_online_count = app_module.obter_usuarios_online_count
    home_routes.get_or_start_dashboard_snapshot = lambda: serialized_snapshot
    app_module.obter_usuarios_online_count = lambda: 0

    try:
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["usuario_logado"] = "perf-probe"
                session["nome_usuario"] = "Perf Probe"
                session["tipo_id"] = 2
                session["permissoes"] = ["dashboard.operacional_acessar"]
                # Sem usuario_id: evita escrita em usuarios_online.

            start_html = time.perf_counter()
            response = client.get("/")
            html_ms = (time.perf_counter() - start_html) * 1000
    finally:
        home_routes.get_or_start_dashboard_snapshot = original_snapshot_getter
        app_module.obter_usuarios_online_count = original_online_count

    return {
        "browser_json_bytes": len(embedded_json.encode("utf-8")),
        "browser_json_render_ms": json_ms,
        "html_bytes": len(response.data or b""),
        "html_render_ms": html_ms,
        "http_status": response.status_code,
        "browser_dashboard_rows": len(dashboard_rows) if isinstance(dashboard_rows, list) else 0,
        "browser_filter_types": len(filter_types) if isinstance(filter_types, dict) else 0,
    }


def measure_status_endpoint(repetitions: int) -> dict[str, Any]:
    from app import app

    latencies_ms: list[float] = []
    response_sizes: list[int] = []
    statuses: Counter[int] = Counter()

    with app.test_client() as client:
        with client.session_transaction() as session:
            session["usuario_logado"] = "perf-probe"
            session["nome_usuario"] = "Perf Probe"
            session["tipo_id"] = 2
            session["permissoes"] = ["dashboard.operacional_acessar"]
            # Sem usuario_id: evita escrita em usuarios_online no before_request.

        for _ in range(max(1, repetitions)):
            start = time.perf_counter()
            response = client.get("/dashboard/status", headers={"Accept": "application/json"})
            latencies_ms.append((time.perf_counter() - start) * 1000)
            response_sizes.append(len(response.data or b""))
            statuses[response.status_code] += 1

    return {
        "status_calls": sum(statuses.values()),
        "http_statuses": dict(statuses),
        "latency_ms_avg": statistics.mean(latencies_ms),
        "latency_ms_min": min(latencies_ms),
        "latency_ms_max": max(latencies_ms),
        "response_bytes_avg": statistics.mean(response_sizes),
    }


def measure_live_ofs() -> dict[str, Any]:
    service = _load_service()

    activity_maps_start = time.perf_counter()
    activity_maps = service._load_activity_type_maps()
    activity_maps_ms = (time.perf_counter() - activity_maps_start) * 1000
    activity_codes = sorted(activity_maps["b2c_codes"].union(activity_maps["redes_codes"]))

    today = service._today()
    date_from = service._date_text(today - service.timedelta(days=7))
    date_to = service._date_text(today)

    original_get = service.requests.get
    original_progress = service._update_progress
    calls: list[dict[str, Any]] = []

    def counted_get(*args, **kwargs):
        params = kwargs.get("params") or []
        param_map = dict(params) if isinstance(params, (list, tuple)) else dict(params)
        calls.append(
            {
                "date": param_map.get("dateFrom"),
                "offset": param_map.get("offset"),
                "limit": param_map.get("limit"),
            }
        )
        return original_get(*args, **kwargs)

    service.requests.get = counted_get
    service._update_progress = lambda *_args, **_kwargs: None

    try:
        fetch_start = time.perf_counter()
        rows = service._fetch_dashboard_activities(date_from, date_to, activity_codes)
        fetch_ms = (time.perf_counter() - fetch_start) * 1000

        build_start = time.perf_counter()
        payload = service._build_payload(rows, activity_maps)
        build_ms = (time.perf_counter() - build_start) * 1000

        serialize_start = time.perf_counter()
        payload_json = service._json_dumps(payload)
        serialize_ms = (time.perf_counter() - serialize_start) * 1000
    finally:
        service.requests.get = original_get
        service._update_progress = original_progress

    pages_by_day = Counter(str(call.get("date") or "") for call in calls)

    return {
        "date_from": date_from,
        "date_to": date_to,
        "activity_type_codes": len(activity_codes),
        "activity_maps_ms": activity_maps_ms,
        "ofs_calls": len(calls),
        "ofs_pages": len(calls),
        "pages_by_day": dict(sorted(pages_by_day.items())),
        "activities_received_deduplicated": len(rows),
        "fetch_ms": fetch_ms,
        "build_payload_ms": build_ms,
        "serialize_payload_ms": serialize_ms,
        "generation_without_persist_ms": activity_maps_ms + fetch_ms + build_ms + serialize_ms,
        "generated_payload_bytes": len(payload_json.encode("utf-8")),
        "generated_dashboard_rows": len(payload.get("dashboard_rows") or []),
        "generated_filter_types": len(((payload.get("filter_read_model") or {}).get("types") or {})),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Baseline seguro do Dashboard Operacional")
    parser.add_argument("--repetitions", type=int, default=5, help="repetições para leituras/status (padrão: 5)")
    parser.add_argument("--render", action="store_true", help="mede JSON Jinja e HTML renderizado sem chamar a rota /")
    parser.add_argument("--status", action="store_true", help="mede /dashboard/status com test client")
    parser.add_argument(
        "--live-ofs",
        action="store_true",
        help="faz consultas OFS de leitura e mede fetch/build; não salva snapshot nem progresso",
    )
    args = parser.parse_args()

    try:
        print("=== Dashboard Performance Probe ===")
        snapshot = measure_snapshot(args.repetitions)
        _print_metric("snapshot_status", snapshot["status"])
        _print_metric("payload_json_text_db", _bytes_text(snapshot["payload_text_bytes_db"]))
        _print_metric("payload_json_storage_db", _bytes_text(snapshot["payload_storage_bytes_db"]))
        _print_metric("payload_json_serialized_python", _bytes_text(snapshot["payload_serialized_bytes_python"]))
        _print_metric("dashboard_rows_db", snapshot["dashboard_rows_db"])
        _print_metric("dashboard_rows_python", snapshot["dashboard_rows_python"])
        _print_metric("filter_types_db", snapshot["filter_types_db"])
        _print_metric("filter_types_python", snapshot["filter_types_python"])
        _print_metric("snapshot_read_avg", f"{snapshot['snapshot_read_ms_avg']:.2f}", "ms")
        _print_metric("snapshot_read_min", f"{snapshot['snapshot_read_ms_min']:.2f}", "ms")
        _print_metric("snapshot_read_max", f"{snapshot['snapshot_read_ms_max']:.2f}", "ms")
        generation_ms = snapshot.get("last_generation_ms")
        _print_metric(
            "last_snapshot_generation",
            "n/a" if generation_ms is None else f"{float(generation_ms):.2f}",
            "" if generation_ms is None else "ms",
        )

        if args.render:
            print("\n=== Render Jinja/HTML ===")
            render = measure_render()
            _print_metric("http_status", render["http_status"])
            _print_metric("browser_json", _bytes_text(render["browser_json_bytes"]))
            _print_metric("browser_json_render", f"{render['browser_json_render_ms']:.2f}", "ms")
            _print_metric("browser_dashboard_rows", render["browser_dashboard_rows"])
            _print_metric("browser_filter_types", render["browser_filter_types"])
            _print_metric("html_response", _bytes_text(render["html_bytes"]))
            _print_metric("html_render", f"{render['html_render_ms']:.2f}", "ms")

        if args.status:
            print("\n=== /dashboard/status ===")
            status = measure_status_endpoint(args.repetitions)
            _print_metric("calls", status["status_calls"])
            _print_metric("http_statuses", status["http_statuses"])
            _print_metric("latency_avg", f"{status['latency_ms_avg']:.2f}", "ms")
            _print_metric("latency_min", f"{status['latency_ms_min']:.2f}", "ms")
            _print_metric("latency_max", f"{status['latency_ms_max']:.2f}", "ms")
            _print_metric("response_size_avg", _bytes_text(status["response_bytes_avg"]))

        if args.live_ofs:
            print("\n=== Atualização OFS (somente leitura, sem persistência) ===")
            live = measure_live_ofs()
            _print_metric("date_range", f"{live['date_from']}..{live['date_to']}")
            _print_metric("activity_type_codes", live["activity_type_codes"])
            _print_metric("ofs_calls", live["ofs_calls"])
            _print_metric("ofs_pages", live["ofs_pages"])
            _print_metric("pages_by_day", live["pages_by_day"])
            _print_metric("activities_deduplicated", live["activities_received_deduplicated"])
            _print_metric("activity_maps", f"{live['activity_maps_ms']:.2f}", "ms")
            _print_metric("ofs_fetch", f"{live['fetch_ms']:.2f}", "ms")
            _print_metric("build_payload", f"{live['build_payload_ms']:.2f}", "ms")
            _print_metric("serialize_payload", f"{live['serialize_payload_ms']:.2f}", "ms")
            _print_metric("generation_without_persist", f"{live['generation_without_persist_ms']:.2f}", "ms")
            _print_metric("generated_payload", _bytes_text(live["generated_payload_bytes"]))
            _print_metric("generated_dashboard_rows", live["generated_dashboard_rows"])
            _print_metric("generated_filter_types", live["generated_filter_types"])

        return 0
    except Exception as exc:  # operação: mensagem sanitizada, sem traceback/payload
        print(f"ERRO: {type(exc).__name__}: {_redact(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
