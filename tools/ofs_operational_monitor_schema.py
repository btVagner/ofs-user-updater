import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

SQL_DIR = ROOT_DIR / "database" / "sql"
FILES = {
    "apply": SQL_DIR / "20260925_ofs_operational_monitor_apply.sql",
    "validate": SQL_DIR / "20260925_ofs_operational_monitor_validate.sql",
    "rollback": SQL_DIR / "20260925_ofs_operational_monitor_rollback.sql",
}
ROLLBACK_CONFIRMATION = "DROP_OFS_OPERATIONAL_MONITOR"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aplica ou valida o schema do Monitor Operacional OFS compartilhado."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--apply", action="store_true", help="Aplica a migração idempotente.")
    mode.add_argument("--validate", action="store_true", help="Executa somente consultas de validação.")
    mode.add_argument("--rollback", action="store_true", help="Remove o schema do monitor; exige --confirm.")
    parser.add_argument("--confirm", default=None)
    return parser.parse_args()


def _statements(path: Path):
    cleaned_lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        cleaned_lines.append(line)
    for statement in "\n".join(cleaned_lines).split(";"):
        statement = statement.strip()
        if statement:
            yield statement


def run_sql(mode: str) -> dict:
    from database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    result_sets = []
    executed = 0
    try:
        for statement in _statements(FILES[mode]):
            cur.execute(statement)
            executed += 1
            if cur.with_rows:
                result_sets.append({
                    "columns": list(cur.column_names),
                    "rows": list(cur.fetchall() or []),
                })
        conn.commit()
        return {
            "mode": mode,
            "sql_file": str(FILES[mode].relative_to(ROOT_DIR)),
            "statements_executed": executed,
            "result_sets": result_sets,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    args = parse_args()
    if args.rollback and args.confirm != ROLLBACK_CONFIRMATION:
        print(
            f"Rollback bloqueado. Use --confirm {ROLLBACK_CONFIRMATION} somente após interromper web e worker.",
            file=sys.stderr,
        )
        return 2
    mode = "apply" if args.apply else "validate" if args.validate else "rollback"
    print(json.dumps(run_sql(mode), ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

