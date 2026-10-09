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
    mode: SQL_DIR / f"20261009_ofs_atividades_notdone_status_{mode}.sql"
    for mode in ("apply", "validate", "rollback")
}
ROLLBACK_CONFIRMATION = "DROP_NOTDONE_ACTIVITY_STATUS"


def _statements(path):
    lines = [line for line in path.read_text(encoding="utf-8").splitlines()
             if line.strip() and not line.lstrip().startswith("--")]
    return [part.strip() for part in "\n".join(lines).split(";") if part.strip()]


def run_sql(mode):
    from database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    result_sets = []
    statements = _statements(FILES[mode])
    try:
        for statement in statements:
            cur.execute(statement)
            if cur.with_rows:
                result_sets.append(list(cur.fetchall() or []))
        conn.commit()
        return {"mode": mode, "statements_executed": len(statements), "result_sets": result_sets}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Coluna de status da OS em atividades não concluídas.")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--apply", action="store_true")
    modes.add_argument("--validate", action="store_true")
    modes.add_argument("--rollback", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.rollback and args.confirm != ROLLBACK_CONFIRMATION:
        parser.error(f"Rollback exige --confirm {ROLLBACK_CONFIRMATION} após voltar o código.")
    mode = "apply" if args.apply else "validate" if args.validate else "rollback"
    print(json.dumps(run_sql(mode), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
