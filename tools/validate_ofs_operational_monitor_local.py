import json
import sys
from datetime import datetime, time, timezone
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from database.connection import get_connection  # noqa: E402
from services.ofs_operational_monitor_service import (  # noqa: E402
    MonitorRefreshCooldown,
    OperationalMonitorService,
    operational_work_date,
)


SCOPE_KEY = "casa-cliente"
ROOT_RESOURCE_ID = "02"
RESOURCE_ID = "__OFS_MONITOR_LOCAL_TEST__"
ACTIVITY_IDS = ("__OFS_MONITOR_LATE_TEST__", "__OFS_MONITOR_SLOT_TEST__")
ACTOR = "codex-local-integration"


def _preflight():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM ofs_operational_monitor_snapshot WHERE scope_key=%s",
            (SCOPE_KEY,),
        )
        if cur.fetchone()[0]:
            raise RuntimeError("Já existe snapshot local; validação cancelada para não substituí-lo.")
        cur.execute(
            "SELECT COUNT(*) FROM ofs_resource_hierarchy WHERE resource_id=%s",
            (RESOURCE_ID,),
        )
        if cur.fetchone()[0]:
            raise RuntimeError("Fixture de validação já existe; remova-a antes de repetir.")
    finally:
        cur.close()
        conn.close()


def _insert_fixture(work_date):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start_of_day = datetime.combine(work_date, time(0, 0))
    outside_slot = datetime.combine(work_date, time(12, 30))
    shift_start = datetime.combine(work_date, time(0, 0))
    shift_end = datetime.combine(work_date, time(23, 59))
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ofs_resource_hierarchy
                (resource_id,parent_resource_id,resource_name,resource_type,status,timezone,
                 depth,root_resource_id,last_seen_at,updated_at)
            VALUES (%s,%s,%s,'TCV','active','America/Sao_Paulo',1,%s,%s,%s)
            """,
            (RESOURCE_ID, ROOT_RESOURCE_ID, "Técnico validação local", ROOT_RESOURCE_ID, now, now),
        )
        cur.execute(
            """
            INSERT INTO ofs_technician_operational_state
                (work_date,resource_id,route_state,route_started_at,calendar_record_type,
                 calendar_start_at,calendar_end_at,resource_timezone_iana,last_reconciled_at,updated_at)
            VALUES (%s,%s,'active',%s,'working',%s,%s,'America/Sao_Paulo',%s,%s)
            """,
            (work_date, RESOURCE_ID, shift_start, shift_start, shift_end, now, now),
        )
        cur.execute(
            """
            INSERT INTO ofs_activity_operational_state
                (activity_id,work_date,resource_id,status,appt_number,activity_type,record_type,
                 start_time,duration_minutes,time_slot,is_black,customer_name,customer_state,resource_timezone_iana,
                 last_reconciled_at,updated_at)
            VALUES (%s,%s,%s,'started','OS-LOCAL-1','INST','regular',%s,1,'08:00-12:00',1,
                    'Cliente validação','SP','America/Sao_Paulo',%s,%s)
            """,
            (ACTIVITY_IDS[0], work_date, RESOURCE_ID, start_of_day, now, now),
        )
        cur.execute(
            """
            INSERT INTO ofs_activity_operational_state
                (activity_id,work_date,resource_id,status,appt_number,activity_type,record_type,
                 start_time,duration_minutes,time_slot,is_black,customer_name,customer_state,resource_timezone_iana,
                 last_reconciled_at,updated_at)
            VALUES (%s,%s,%s,'pending','OS-LOCAL-2','MAN','regular',%s,30,'08:00-12:00',0,
                    'Cliente slot','RS','America/Sao_Paulo',%s,%s)
            """,
            (ACTIVITY_IDS[1], work_date, RESOURCE_ID, outside_slot, now, now),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _cleanup():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM ofs_operational_monitor_refresh_log WHERE requested_by_username=%s",
            (ACTOR,),
        )
        cur.execute(
            "DELETE FROM ofs_operational_monitor_snapshot WHERE scope_key=%s AND requested_by_username=%s",
            (SCOPE_KEY, ACTOR),
        )
        cur.execute(
            "DELETE FROM ofs_activity_operational_state WHERE activity_id IN (%s,%s)",
            ACTIVITY_IDS,
        )
        cur.execute(
            "DELETE FROM ofs_technician_operational_state WHERE resource_id=%s",
            (RESOURCE_ID,),
        )
        cur.execute(
            "DELETE FROM ofs_resource_hierarchy WHERE resource_id=%s",
            (RESOURCE_ID,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main():
    _preflight()
    work_date = operational_work_date()
    report = {"work_date": work_date.isoformat(), "ofs_api_calls": 0}
    try:
        _insert_fixture(work_date)
        service = OperationalMonitorService()
        snapshot = service.refresh(SCOPE_KEY, actor_id=None, actor_username=ACTOR)
        payload = snapshot.get("payload") or {}
        late_ids = {row.get("id") for row in payload.get("late_candidates") or []}
        slot_ids = {row.get("id") for row in payload.get("slot") or []}
        black_ids = {row.get("id") for row in payload.get("black") or []}
        if snapshot.get("status") != "ready" or not snapshot.get("has_payload"):
            raise AssertionError("Snapshot não foi persistido como ready.")
        if ACTIVITY_IDS[0] not in late_ids or ACTIVITY_IDS[0] not in black_ids:
            raise AssertionError("Atividade de prazo/Black não apareceu no snapshot.")
        if ACTIVITY_IDS[1] not in slot_ids:
            raise AssertionError("Atividade fora do slot não apareceu no snapshot.")

        cooldown_confirmed = False
        try:
            service.refresh(SCOPE_KEY, actor_id=None, actor_username=ACTOR)
        except MonitorRefreshCooldown:
            cooldown_confirmed = True
        if not cooldown_confirmed:
            raise AssertionError("A segunda atualização não foi bloqueada pelo cooldown.")

        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT status,CHAR_LENGTH(payload_json),TIMESTAMPDIFF(SECOND,refreshed_at,expires_at) "
                "FROM ofs_operational_monitor_snapshot WHERE scope_key=%s",
                (SCOPE_KEY,),
            )
            status, payload_bytes, ttl_seconds = cur.fetchone()
        finally:
            cur.close()
            conn.close()

        report.update({
            "snapshot_status": status,
            "payload_characters": payload_bytes,
            "ttl_seconds": ttl_seconds,
            "cooldown_confirmed": cooldown_confirmed,
            "late_fixture_found": ACTIVITY_IDS[0] in late_ids,
            "slot_fixture_found": ACTIVITY_IDS[1] in slot_ids,
            "black_fixture_found": ACTIVITY_IDS[0] in black_ids,
        })
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        _cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
