import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")


from database.connection import get_connection
from services.bi_activities_service import (
    purge_old_bi_activity_data,
    run_collection,
)


LOCK_NAME = "ofs_bi_activities_cron_lock"

CRON_STATUSES = [
    "completed",
    "notdone",
    "pending",
    "started",
    "cancelled",
    "enroute",
]

CRON_RESOURCES = "02"


def acquire_lock():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT GET_LOCK(%s, 0)", (LOCK_NAME,))
        row = cursor.fetchone()
        acquired = bool(row and row[0] == 1)

        if not acquired:
            cursor.close()
            conn.close()
            return None, None

        return conn, cursor

    except Exception:
        cursor.close()
        conn.close()
        raise

def release_lock(conn, cursor):
    try:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
        cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def main():
    lock_conn = None
    lock_cursor = None

    try:
        lock_conn, lock_cursor = acquire_lock()

        if not lock_conn:
            print("[BI_ACTIVITIES_CRON] Já existe uma coleta em execução. Encerrando.")
            return 0

        today = date.today()

        purge_result = purge_old_bi_activity_data(keep_date=today)
        print(f"[BI_ACTIVITIES_CRON] Limpeza: {purge_result}")

        result = run_collection(
            date_from=today,
            date_to=today,
            statuses=CRON_STATUSES,
            resources=CRON_RESOURCES,
            activity_types=None,
            created_by_user_id=None,
            created_by_username="cron",
        )

        print(f"[BI_ACTIVITIES_CRON] Coleta concluída: {result}")
        return 0

    except Exception as exc:
        print(f"[BI_ACTIVITIES_CRON] ERRO: {exc}")
        return 1

    finally:
        if lock_conn and lock_cursor:
            release_lock(lock_conn, lock_cursor)


if __name__ == "__main__":
    raise SystemExit(main())