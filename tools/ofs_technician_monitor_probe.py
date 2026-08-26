from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterable, List

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from services.ofs_technician_monitor_service import (  # noqa: E402
    MySQLTechnicianMonitorRepository,
    TechnicianMonitorService,
    default_monitor_work_date,
)


def _parse_args():
    parser = argparse.ArgumentParser(description="Mede as APIs locais do monitor de técnicos (Demanda 08).")
    parser.add_argument("--date", help="Data operacional YYYY-MM-DD. Padrão: hoje no timezone central.")
    parser.add_argument("--repetitions", type=int, default=10, help="Repetições para média/min/max/p50/p95.")
    parser.add_argument("--http", action="store_true", help="Mede também os endpoints Flask via test_client.")
    parser.add_argument("--explain", action="store_true", help="Executa EXPLAIN das três queries de leitura.")
    return parser.parse_args()


def _percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _stats(values: List[float]) -> dict:
    return {
        "avg_ms": round(statistics.fmean(values), 3),
        "min_ms": round(min(values), 3),
        "max_ms": round(max(values), 3),
        "p50_ms": round(_percentile(values, 0.50), 3),
        "p95_ms": round(_percentile(values, 0.95), 3),
    }


def _collect_direct(service: TechnicianMonitorService, work_date: date, repetitions: int):
    summary_total: List[float] = []
    tree_total: List[float] = []
    children_total: List[float] = []
    hierarchy_query: List[float] = []
    operational_query: List[float] = []
    health_query: List[float] = []
    classification: List[float] = []
    aggregation_summary: List[float] = []
    aggregation_tree: List[float] = []
    serialization_summary: List[float] = []
    serialization_tree: List[float] = []
    summary_payload = None
    tree_payload = None
    children_payload = None
    summary_metrics = None
    tree_metrics = None
    children_metrics = None

    for _ in range(repetitions):
        started = time.perf_counter()
        summary_payload, summary_metrics = service.build_summary(work_date)
        summary_total.append((time.perf_counter() - started) * 1000.0)

        started = time.perf_counter()
        tree_payload, tree_metrics = service.build_tree(work_date, mode="full")
        tree_total.append((time.perf_counter() - started) * 1000.0)

        started = time.perf_counter()
        children_payload, children_metrics = service.build_tree(work_date, mode="children")
        children_total.append((time.perf_counter() - started) * 1000.0)

        hierarchy_query.append(float(tree_metrics["hierarchy_query_ms"]))
        operational_query.append(float(tree_metrics["operational_query_ms"]))
        health_query.append(float(tree_metrics["health_query_ms"]))
        classification.append(float(tree_metrics["classification_ms"]))
        aggregation_summary.append(float(summary_metrics["aggregation_ms"]))
        aggregation_tree.append(float(tree_metrics["aggregation_ms"]))
        serialization_summary.append(float(summary_metrics["serialization_ms"]))
        serialization_tree.append(float(tree_metrics["serialization_ms"]))

    assert summary_payload is not None
    assert tree_payload is not None
    assert children_payload is not None
    assert summary_metrics is not None
    assert tree_metrics is not None
    assert children_metrics is not None

    return {
        "counts": {
            "nodes": tree_payload["total_nodes"],
            "technicians": tree_payload["total_technicians"],
            "technicians_with_operational_state": tree_payload["technicians_with_operational_state"],
            "technicians_without_operational_state": tree_payload["technicians_without_operational_state"],
        },
        "payload_bytes": {
            "summary": int(summary_metrics["payload_bytes"]),
            "tree_full": int(tree_metrics["payload_bytes"]),
            "tree_children_root": int(children_metrics["payload_bytes"]),
        },
        "timings": {
            "summary_total": _stats(summary_total),
            "tree_full_total": _stats(tree_total),
            "tree_children_root_total": _stats(children_total),
            "hierarchy_query": _stats(hierarchy_query),
            "operational_query": _stats(operational_query),
            "health_query": _stats(health_query),
            "classification": _stats(classification),
            "summary_aggregation": _stats(aggregation_summary),
            "tree_aggregation": _stats(aggregation_tree),
            "summary_serialization": _stats(serialization_summary),
            "tree_serialization": _stats(serialization_tree),
        },
        "health": summary_payload.get("health"),
    }


def _collect_http(work_date: date, repetitions: int):
    import requests
    from app import app

    permission = "dashboard.operacional_acessar"
    results = {"summary": [], "tree_full": [], "tree_children_root": []}
    status_codes = {"summary": [], "tree_full": [], "tree_children_root": []}
    payload_bytes = {}
    external_http_attempts = []

    original_request = requests.sessions.Session.request

    def blocked_request(self, method, url, *args, **kwargs):
        external_http_attempts.append({"method": method, "url": str(url)})
        raise AssertionError("Endpoint HTTP local tentou executar request externo.")

    requests.sessions.Session.request = blocked_request
    try:
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["usuario_logado"] = "d08_probe"
                session["usuario_id"] = -808
                session["tipo_id"] = 2
                session["permissoes"] = [permission]
                session["_last_online_ping"] = int(time.time())

            targets = {
                "summary": f"/dashboard/technicians/summary?date={work_date.isoformat()}",
                "tree_full": f"/dashboard/technicians/tree?date={work_date.isoformat()}&mode=full",
                "tree_children_root": f"/dashboard/technicians/tree?date={work_date.isoformat()}&mode=children",
            }
            for _ in range(repetitions):
                for key, target in targets.items():
                    started = time.perf_counter()
                    response = client.get(target)
                    results[key].append((time.perf_counter() - started) * 1000.0)
                    status_codes[key].append(response.status_code)
                    payload_bytes[key] = len(response.data)
                    if response.status_code != 200:
                        raise RuntimeError(f"{target} retornou HTTP {response.status_code}")
    finally:
        requests.sessions.Session.request = original_request

    return {
        "timings": {key: _stats(values) for key, values in results.items()},
        "status_codes": {key: sorted(set(values)) for key, values in status_codes.items()},
        "payload_bytes": payload_bytes,
        "external_http_attempts": external_http_attempts,
    }


def main():
    args = _parse_args()
    repetitions = max(int(args.repetitions), 1)
    work_date = date.fromisoformat(args.date) if args.date else default_monitor_work_date()

    repository = MySQLTechnicianMonitorRepository()
    service = TechnicianMonitorService(repository=repository)

    report = {
        "work_date": work_date.isoformat(),
        "repetitions": repetitions,
        "direct": _collect_direct(service, work_date, repetitions),
    }

    full_bytes = report["direct"]["payload_bytes"]["tree_full"]
    if full_bytes > 500 * 1024:
        report["tree_strategy_signal"] = {
            "decision": "INCREMENTAL_REQUIRED",
            "reason": "tree_full acima de aproximadamente 500 KiB",
        }
    else:
        report["tree_strategy_signal"] = {
            "decision": "FULL_PAYLOAD_WITHIN_SIZE_THRESHOLD",
            "reason": "tree_full abaixo de aproximadamente 500 KiB; confirmar latência antes da decisão final",
        }

    if args.explain:
        report["explain"] = repository.explain_queries(work_date)
    if args.http:
        report["http"] = _collect_http(work_date, repetitions)

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
