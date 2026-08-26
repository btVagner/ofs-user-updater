import argparse
import json
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from services.ofs_technician_operational_service import MySQLOperationalRepository, LOCK_NAME  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="Probe local seguro do read model operacional; não consulta OFS.")
    parser.add_argument("--date", default=None, help="Data YYYY-MM-DD. Padrão: hoje.")
    parser.add_argument("--check-lock", action="store_true", help="Testa se o lock do worker está livre sem mantê-lo adquirido.")
    parser.add_argument("--output", default=None, help="JSON de saída opcional.")
    return parser.parse_args()


def lock_status():
    from database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()
    acquired = False
    try:
        cur.execute("SELECT GET_LOCK(%s,0)", (LOCK_NAME,))
        row = cur.fetchone()
        acquired = bool(row and row[0] == 1)
        return {"lock_name": LOCK_NAME, "available": acquired}
    finally:
        if acquired:
            cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
            cur.fetchone()
        cur.close()
        conn.close()


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date) if args.date else date.today()
    repo = MySQLOperationalRepository()
    report = {
        "probe": "ofs_technician_operational_probe",
        "date": target_date.isoformat(),
        "ofs_calls": 0,
        "metrics": repo.operational_metrics(target_date),
    }
    if args.check_lock:
        report["lock"] = lock_status()

    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        path = Path(args.output)
        if not path.is_absolute():
            path = ROOT_DIR / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OFS_OPERATIONAL_READ_MODEL_PROBE] relatório salvo em: {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
