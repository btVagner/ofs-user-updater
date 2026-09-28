"""Aplica/valida somente as tabelas e permissões das tratativas locais."""

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")
FILES = {
    "apply": ROOT_DIR / "database/sql/20260928_ofs_operational_monitor_treatment_apply.sql",
    "validate": ROOT_DIR / "database/sql/20260928_ofs_operational_monitor_treatment_validate.sql",
}


def _statements(path):
    lines = [line for line in path.read_text(encoding="utf-8").splitlines()
             if line.strip() and not line.lstrip().startswith("--")]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


def run_sql(mode):
    from database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    results = []
    try:
        statements = _statements(FILES[mode])
        for statement in statements:
            cur.execute(statement)
            if cur.with_rows:
                results.append({"columns": list(cur.column_names), "rows": list(cur.fetchall() or [])})
        if mode == "apply":
            conn.commit()
        else:
            conn.rollback()
        return {"mode": mode, "sql_file": str(FILES[mode].relative_to(ROOT_DIR)),
                "statements_executed": len(statements), "result_sets": results}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Schema de tratativas do monitor, sem chamadas ao OFS.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--apply", action="store_true")
    group.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_sql("apply" if args.apply else "validate"), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
