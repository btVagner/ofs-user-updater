from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from services.ofs_technician_alert_service import (  # noqa: E402
    AlertRuleSettings,
    MySQLTechnicianAlertReadRepository,
    TechnicianAlertClassifier,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark local do classificador operacional; nunca consulta OFS.")
    parser.add_argument("--date", default=None, help="Data YYYY-MM-DD. Padrão: hoje.")
    parser.add_argument("--repetitions", type=int, default=20, help="Repetições do benchmark em lote.")
    parser.add_argument("--synthetic", type=int, default=0, help="Usa N técnicos sintéticos e dispensa MySQL.")
    return parser.parse_args()


def synthetic_snapshot(total: int, work_date: date, now: datetime):
    rows = []
    for index in range(total):
        rows.append({
            "work_date": work_date,
            "resource_id": f"SYN{index:04d}",
            "resource_timezone_iana": "America/Sao_Paulo",
            "calendar_record_type": "working",
            "calendar_start_at": datetime.combine(work_date, datetime.min.time()).replace(hour=8),
            "calendar_end_at": datetime.combine(work_date, datetime.min.time()).replace(hour=17),
            "route_state": "active",
            "route_started_at": datetime.combine(work_date, datetime.min.time()).replace(hour=8, minute=2),
            "route_reactivated_at": None,
            "route_ended_at": None,
            "started_count": 0,
            "suspended_count": 0,
            "open_activity_count": 0,
        })
    marker = now.astimezone(timezone.utc).replace(tzinfo=None)
    health = {
        "events": {"status": "ok", "last_success_at": marker - timedelta(seconds=30), "caught_up": True},
        "activities": {"status": "ok", "last_success_at": marker - timedelta(minutes=5)},
        "calendars": {"status": "ok", "last_success_at": marker - timedelta(minutes=20)},
        "routes": {"status": "ok", "last_success_at": marker - timedelta(hours=12)},
    }
    return rows, health


def main():
    args = parse_args()
    work_date = date.fromisoformat(args.date) if args.date else date.today()
    now = datetime.now(timezone.utc)
    classifier = TechnicianAlertClassifier(AlertRuleSettings.from_env())

    if args.synthetic > 0:
        rows, health = synthetic_snapshot(args.synthetic, work_date, now)
        source = "synthetic"
    else:
        rows, health = MySQLTechnicianAlertReadRepository().load_snapshot(work_date)
        source = "mysql"

    repetitions = max(args.repetitions, 1)
    elapsed_ms = []
    result = []
    for _ in range(repetitions):
        started = time.perf_counter()
        result = classifier.classify_batch(rows, health, now=now)
        elapsed_ms.append((time.perf_counter() - started) * 1000.0)

    severities = {}
    for item in result:
        severities[item["operational_severity"]] = severities.get(item["operational_severity"], 0) + 1

    output = {
        "source": source,
        "work_date": work_date.isoformat(),
        "technicians": len(rows),
        "repetitions": repetitions,
        "classification_ms_avg": round(statistics.mean(elapsed_ms), 3),
        "classification_ms_min": round(min(elapsed_ms), 3),
        "classification_ms_max": round(max(elapsed_ms), 3),
        "classification_us_per_technician_avg": round((statistics.mean(elapsed_ms) * 1000.0 / len(rows)), 3) if rows else 0,
        "severity_counts": severities,
        "ofs_calls": 0,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
