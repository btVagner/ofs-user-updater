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
    "apply": SQL_DIR / "20260826_ofs_technician_operational_apply.sql",
    "validate": SQL_DIR / "20260826_ofs_technician_operational_validate.sql",
    "rollback": SQL_DIR / "20260826_ofs_technician_operational_rollback.sql",
}
ROLLBACK_CONFIRMATION = "D06_DROP_OPERATIONAL_STATE"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aplica/valida o schema da Demanda 06 usando a conexão MySQL configurada no projeto."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--apply", action="store_true", help="Cria as tabelas operacionais da Demanda 06.")
    mode.add_argument("--validate", action="store_true", help="Executa apenas consultas seguras de validação.")
    mode.add_argument("--rollback", action="store_true", help="Remove as tabelas da Demanda 06; exige --confirm.")
    parser.add_argument(
        "--confirm",
        default=None,
        help=f"Confirmação obrigatória do rollback: {ROLLBACK_CONFIRMATION}",
    )
    return parser.parse_args()


def _statements(path: Path):
    text = path.read_text(encoding="utf-8")
    cleaned_lines = []
    for line in text.splitlines():
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

    path = FILES[mode]
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    result_sets = []
    executed = 0
    try:
        for statement in _statements(path):
            cur.execute(statement)
            executed += 1
            if cur.with_rows:
                rows = list(cur.fetchall() or [])
                result_sets.append({"columns": list(cur.column_names), "rows": rows})
        conn.commit()
        return {
            "mode": mode,
            "sql_file": str(path.relative_to(ROOT_DIR)),
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
            f"Rollback bloqueado. Reexecute com --confirm {ROLLBACK_CONFIRMATION} somente se realmente quiser remover o read model D06.",
            file=sys.stderr,
        )
        return 2

    mode = "apply" if args.apply else "validate" if args.validate else "rollback"
    report = run_sql(mode)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
