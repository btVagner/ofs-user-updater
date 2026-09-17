from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from services.ofs_technician_operational_service import (  # noqa: E402
    MySQLOperationalRepository,
    OFSOperationalAPI,
    OperationalAlreadyRunning,
    OperationalSettings,
    mysql_operational_lock,
    normalize_route_baseline,
    sanitize_operational_error,
    utc_now_naive,
)


ROUTE_TIMESTAMP_FIELDS = (
    "route_started_at",
    "route_reactivated_at",
    "route_ended_at",
)
MAX_SAMPLE_CHANGES = 20


def _iso(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _write_evidence(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=_iso) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def default_evidence_path(work_date: date, apply: bool) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    mode = "apply" if apply else "dry-run"
    return Path(tempfile.gettempdir()) / f"ofs_route_timestamp_repair_{work_date.isoformat()}_{mode}_{stamp}.json"


def repair_route_timestamps(
    work_date: date,
    *,
    repository,
    api,
    apply: bool = False,
    evidence_path: Optional[Path] = None,
) -> dict:
    evidence_path = Path(evidence_path or default_evidence_path(work_date, apply)).resolve()
    rows = repository.get_route_timestamp_rows(work_date)
    changes = []
    errors = []
    api_calls = 0
    skipped_source_null = 0

    for row in rows:
        resource_id = str(row["resource_id"])
        try:
            payload, calls = api.get_route(resource_id, work_date)
            api_calls += int(calls or 0)
            source = normalize_route_baseline(
                resource_id,
                work_date,
                payload,
                reconciled_at=utc_now_naive(),
            )
            for field in ROUTE_TIMESTAMP_FIELDS:
                value_before = row.get(field)
                value_after = source.get(field)
                if value_after is None:
                    skipped_source_null += 1
                    continue
                if value_before != value_after:
                    changes.append(
                        {
                            "resource_id": resource_id,
                            "work_date": work_date.isoformat(),
                            "campo": field,
                            "valor_anterior": value_before,
                            "valor_novo": value_after,
                            "applied": False,
                        }
                    )
        except Exception as exc:
            errors.append(
                {
                    "resource_id": resource_id,
                    "work_date": work_date.isoformat(),
                    "error": sanitize_operational_error(exc),
                }
            )

    resource_ids_with_changes = {change["resource_id"] for change in changes}
    report = {
        "mode": "apply" if apply else "dry-run",
        "work_date": work_date.isoformat(),
        "analyzed_resources": len(rows),
        "estimated_minimum_api_calls": len(rows),
        "api_calls": api_calls,
        "resources_with_divergences": len(resource_ids_with_changes),
        "divergent_fields": len(changes),
        "corrected_resources": 0,
        "corrected_fields": 0,
        "errors_count": len(errors),
        "skipped_source_null_fields": skipped_source_null,
        "sample": changes[:MAX_SAMPLE_CHANGES],
        "errors": errors,
        "changes": changes,
        "evidence_file": str(evidence_path),
    }
    _write_evidence(evidence_path, report)

    if apply:
        changes_by_resource = defaultdict(list)
        for change in changes:
            changes_by_resource[change["resource_id"]].append(change)
        for resource_id, resource_changes in changes_by_resource.items():
            values = {change["campo"]: change["valor_novo"] for change in resource_changes}
            try:
                repository.update_route_timestamps(work_date, resource_id, values, now=utc_now_naive())
                for change in resource_changes:
                    change["applied"] = True
                report["corrected_resources"] += 1
                report["corrected_fields"] += len(resource_changes)
            except Exception as exc:
                report["errors"].append(
                    {
                        "resource_id": resource_id,
                        "work_date": work_date.isoformat(),
                        "error": sanitize_operational_error(exc),
                    }
                )
                report["errors_count"] = len(report["errors"])
            report["sample"] = changes[:MAX_SAMPLE_CHANGES]
            _write_evidence(evidence_path, report)

    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Reconcilia timestamps locais de rota OFS contra o GET Route; dry-run por padrao."
    )
    parser.add_argument("--date", required=True, help="Data obrigatoria no formato YYYY-MM-DD.")
    parser.add_argument("--apply", action="store_true", help="Aplica somente as divergencias comprovadas.")
    parser.add_argument(
        "--evidence-file",
        type=Path,
        default=None,
        help="Arquivo JSON de evidencia/rollback. Padrao: diretorio temporario do sistema.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        work_date = date.fromisoformat(args.date)
    except ValueError:
        print("ERRO: --date deve usar YYYY-MM-DD", file=sys.stderr)
        return 2

    repository = MySQLOperationalRepository()
    settings = OperationalSettings.from_env()
    api = OFSOperationalAPI(settings=settings)
    try:
        with mysql_operational_lock():
            report = repair_route_timestamps(
                work_date,
                repository=repository,
                api=api,
                apply=args.apply,
                evidence_path=args.evidence_file,
            )
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=_iso))
        return 1 if report["errors_count"] else 0
    except OperationalAlreadyRunning as exc:
        print(f"ERRO: {sanitize_operational_error(exc)}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"ERRO: {sanitize_operational_error(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
