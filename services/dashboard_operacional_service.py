import json
import os
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo
import requests

from database.connection import get_connection
from ofs.client import OFSClient
from services.online_service import obter_usuarios_online_count
from services.ofs_os_report_service import (
    API_LIMIT,
    REQUEST_TIMEOUT,
    _build_or_equals_query,
    _iter_date_strings,
    _normalize_items_payload,
)


SNAPSHOT_KEY = "home_dashboard"
SNAPSHOT_TTL_MINUTES = 15
RUNNING_STALE_MINUTES = 15
DEFAULT_RESOURCES = "02"

STATUS_OPTIONS = [
    "completed",
    "notdone",
    "pending",
    "started",
    "suspended",
    "cancelled",
    "enroute",
]

REDES_CODES = {"INF_COR", "INF_PRE", "MAN_COR", "MAN_PRE"}
DASHBOARD_TIMEZONE = os.getenv("DASHBOARD_TIMEZONE", "America/Sao_Paulo")

def _now():
    return datetime.now(ZoneInfo(DASHBOARD_TIMEZONE)).replace(tzinfo=None)


def _today():
    return _now().date()


def _dt_text(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _date_text(value):
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value or "")


def _json_dumps(value):
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value):
    if not value:
        return None
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def _ensure_snapshot_table():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_operacional_snapshot (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                snapshot_key VARCHAR(80) NOT NULL UNIQUE,
                status VARCHAR(20) NOT NULL DEFAULT 'completed',
                payload_json JSON NULL,
                error_text TEXT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NULL,
                started_at DATETIME NULL,
                finished_at DATETIME NULL
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _load_snapshot():
    _ensure_snapshot_table()

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT
                snapshot_key,
                status,
                payload_json,
                error_text,
                updated_at,
                expires_at,
                started_at,
                finished_at,
                progress_percent,
                progress_message,
                progress_updated_at
            FROM dashboard_operacional_snapshot
            WHERE snapshot_key = %s
            """,
            (SNAPSHOT_KEY,),
        )
        row = cur.fetchone()

        if not row:
            return None

        row["payload"] = _json_loads(row.get("payload_json"))
        return row
    finally:
        cur.close()
        conn.close()


def _running_is_stale(snapshot):
    if not snapshot or snapshot.get("status") != "running":
        return False

    started_at = snapshot.get("started_at")
    if not started_at:
        return True

    return (_now() - started_at) > timedelta(minutes=RUNNING_STALE_MINUTES)


def _snapshot_is_valid(snapshot):
    if not snapshot or snapshot.get("status") != "completed":
        return False

    expires_at = snapshot.get("expires_at")
    if not expires_at:
        return False

    return expires_at > _now()


def _try_mark_running():
    stale_limit = _now() - timedelta(minutes=RUNNING_STALE_MINUTES)
    conn = get_connection()
    cur = conn.cursor()

    try:
        try:
            cur.execute(
                """
                INSERT INTO dashboard_operacional_snapshot (
                    snapshot_key,
                    status,
                    started_at,
                    updated_at,
                    expires_at,
                    error_text
                )
                VALUES (%s, 'running', %s, %s, NULL, NULL)
                """,
                (SNAPSHOT_KEY, _now(), _now()),
            )
            conn.commit()
            return True

        except Exception as exc:
            if getattr(exc, "errno", None) != 1062:
                conn.rollback()
                raise

            conn.rollback()

        cur.execute(
            """
            UPDATE dashboard_operacional_snapshot
            SET
                status = 'running',
                started_at = %s,
                error_text = NULL
            WHERE snapshot_key = %s
              AND (
                    status <> 'running'
                    OR started_at IS NULL
                    OR started_at < %s
                  )
            """,
            (_now(), SNAPSHOT_KEY, stale_limit),
        )
        changed = cur.rowcount > 0
        conn.commit()
        return changed

    finally:
        cur.close()
        conn.close()

def _update_progress(percent: int, message: str):
    percent = max(0, min(int(percent or 0), 99))
    message = str(message or "").strip()[:255]

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE dashboard_operacional_snapshot
            SET
                progress_percent = %s,
                progress_message = %s,
                progress_updated_at = %s
            WHERE snapshot_key = %s
              AND status = 'running'
            """,
            (percent, message, _now(), SNAPSHOT_KEY),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
def _finish_success(payload):
    now = _now()
    expires_at = now + timedelta(minutes=SNAPSHOT_TTL_MINUTES)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO dashboard_operacional_snapshot (
                snapshot_key,
                status,
                payload_json,
                error_text,
                updated_at,
                expires_at,
                started_at,
                finished_at
            )
            VALUES (%s, 'completed', %s, NULL, %s, %s, NULL, %s)
            ON DUPLICATE KEY UPDATE
                status = 'completed',
                payload_json = VALUES(payload_json),
                error_text = NULL,
                updated_at = VALUES(updated_at),
                expires_at = VALUES(expires_at),
                finished_at = VALUES(finished_at)
            """,
            (SNAPSHOT_KEY, _json_dumps(payload), now, expires_at, now),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _finish_failure(error_text):
    now = _now()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO dashboard_operacional_snapshot (
                snapshot_key,
                status,
                error_text,
                updated_at,
                expires_at,
                started_at,
                finished_at
            )
            VALUES (%s, 'failed', %s, %s, NULL, NULL, %s)
            ON DUPLICATE KEY UPDATE
                status = 'failed',
                error_text = VALUES(error_text),
                finished_at = VALUES(finished_at),
                updated_at = updated_at
            """,
            (SNAPSHOT_KEY, str(error_text or "")[:60000], now, now),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _load_activity_type_maps():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT
                code,
                label_pt,
                category
            FROM ofs_activity_type_map
            WHERE is_active = 1
            """
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    labels = {}
    b2c_codes = set()
    redes_codes = set()

    for row in rows:
        code = str(row.get("code") or "").strip()
        label = str(row.get("label_pt") or code).strip()
        category = str(row.get("category") or "").strip().lower()

        if not code:
            continue

        labels[code] = label or code

        label_lower = label.lower()
        code_lower = code.lower()
        is_retirada = "retir" in label_lower or "retir" in code_lower

        if category == "customer_home" and not is_retirada:
            b2c_codes.add(code)

        if category == "redes" or code in REDES_CODES:
            redes_codes.add(code)

    redes_codes.update(REDES_CODES)

    return {
        "labels": labels,
        "b2c_codes": b2c_codes,
        "redes_codes": redes_codes,
    }


def _fetch_dashboard_activities(date_from: str, date_to: str, activity_codes: List[str]) -> List[dict]:
    if not activity_codes:
        return []

    client = OFSClient()
    url = f"{client.base_url}/activities/"
    headers = {"Accept": "application/json"}
    resources = (os.getenv("DASHBOARD_OFS_RESOURCES") or DEFAULT_RESOURCES).strip()

    fields = [
        "activityId",
        "activityType",
        "status",
        "resourceId",
        "city",
        "date",
        "endTime",
    ]

    q = (
        f"{_build_or_equals_query('status', STATUS_OPTIONS)} "
        f"and {_build_or_equals_query('activityType', activity_codes)}"
    )

    all_items = []
    seen_activity_ids = set()

    days = list(_iter_date_strings(date_from, date_to))
    total_days = max(len(days), 1)

    for day_index, day in enumerate(days, start=1):
        offset = 0
        page = 1
        while True:
            day_base_percent = 10 + int(((day_index - 1) / total_days) * 75)
            _update_progress(
                day_base_percent,
                f"Consultando OFS - {day} - página {page}",
            )
            params = [
                ("dateFrom", day),
                ("dateTo", day),
                ("resources", resources),
                ("q", q),
                ("fields", ",".join(fields)),
                ("limit", str(API_LIMIT)),
                ("offset", str(offset)),
            ]

            response = requests.get(
                url,
                headers=headers,
                params=params,
                auth=client.auth,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()
            items = _normalize_items_payload(data)

            if not items:
                break

            for item in items:
                activity_id = str(item.get("activityId") or "").strip()

                if activity_id:
                    if activity_id in seen_activity_ids:
                        continue
                    seen_activity_ids.add(activity_id)

                all_items.append(item)

            has_more = bool(data.get("hasMore")) if isinstance(data, dict) else False
            if not has_more:
                break

            offset += len(items)
            page += 1
        _update_progress(
            10 + int((day_index / total_days) * 75),
            f"Dia {day_index} de {total_days} concluído",
        )
    return all_items


def _completion_rate(total, completed):
    total = int(total or 0)
    completed = int(completed or 0)
    if total <= 0:
        return 0
    return round((completed / total) * 100, 2)


def _variation_percent(today_total, last_week_total):
    today_total = int(today_total or 0)
    last_week_total = int(last_week_total or 0)

    if last_week_total <= 0:
        return 100.0 if today_total > 0 else 0.0

    return round(((today_total - last_week_total) / last_week_total) * 100, 1)

def _parse_ofs_datetime(value):
    value = str(value or "").strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:19], fmt)
        except ValueError:
            continue

    return None


def _item_finished_until_time(item, until_time):
    if until_time is None:
        return True

    end_dt = _parse_ofs_datetime(item.get("endTime"))
    if not end_dt:
        return False

    return end_dt.time() <= until_time

def _count_by_status(rows, date_value, allowed_codes, status_value, until_time=None):
    total = 0
    status_value = str(status_value or "").strip().lower()

    for item in rows:
        if str(item.get("date") or "").strip() != date_value:
            continue
        if str(item.get("status") or "").strip().lower() != status_value:
            continue
        if str(item.get("activityType") or "").strip() not in allowed_codes:
            continue
        if not _item_finished_until_time(item, until_time):
            continue

        total += 1

    return total


def _count_completed(rows, date_value, allowed_codes, until_time=None):
    return _count_by_status(rows, date_value, allowed_codes, "completed", until_time)


def _list_from_counter(counter, key_name, total_name="total", limit=None):
    rows = [
        {
            key_name: key,
            total_name: total,
        }
        for key, total in counter.items()
    ]
    rows.sort(key=lambda item: item[total_name], reverse=True)
    return rows[:limit] if limit else rows


def _build_payload(rows, activity_maps):
    now = _now()
    today = now.date()
    comparison_until_time = now.time().replace(microsecond=0)
    comparison_until_text = now.strftime("%H:%M")
    last_week_same_day = today - timedelta(days=7)
    last_7_from = today - timedelta(days=6)

    today_text = _date_text(today)
    last_week_text = _date_text(last_week_same_day)

    labels = activity_maps["labels"]
    b2c_codes = activity_maps["b2c_codes"]
    redes_codes = activity_maps["redes_codes"]

    today_by_status = defaultdict(int)
    b2c_by_type_today = defaultdict(int)
    redes_by_type_today = defaultdict(int)
    city_stats = {}
    all_notdone_by_day = defaultdict(int)
    dashboard_rows = []

    for item in rows:
        item_date = str(item.get("date") or "").strip()
        status = str(item.get("status") or "nao_informado").strip().lower() or "nao_informado"
        activity_type = str(item.get("activityType") or "").strip()
        city = str(item.get("city") or "Não informado").strip() or "Não informado"

        dashboard_rows.append({
            "date": item_date,
            "status": status,
            "activityType": activity_type,
            "activityTypeLabel": labels.get(activity_type, activity_type or "Não informado"),
            "city": city,
            "endTime": str(item.get("endTime") or "").strip(),
            "group": (
                "redes"
                if activity_type in redes_codes
                else "b2c"
                if activity_type in b2c_codes
                else "outros"
            ),
        })

        if status == "notdone":
            all_notdone_by_day[item_date] += 1

        if item_date != today_text:
            continue

        today_by_status[status] += 1

        if activity_type in b2c_codes:
            b2c_by_type_today[activity_type] += 1

        if activity_type in redes_codes:
            redes_by_type_today[activity_type] += 1

        if city not in city_stats:
            city_stats[city] = {
                "city": city,
                "total": 0,
                "completed": 0,
                "notdone": 0,
            }

        city_stats[city]["total"] += 1
        if status == "completed":
            city_stats[city]["completed"] += 1
        if status == "notdone":
            city_stats[city]["notdone"] += 1

    b2c_completed_today = _count_completed(rows, today_text, b2c_codes)
    redes_completed_today = _count_completed(rows, today_text, redes_codes)
    b2c_completed_last_week = _count_completed(
        rows,
        last_week_text,
        b2c_codes,
        comparison_until_time,
    )
    redes_completed_last_week = _count_completed(
        rows,
        last_week_text,
        redes_codes,
        comparison_until_time,
    )

    b2c_notdone_today = _count_by_status(rows, today_text, b2c_codes, "notdone")
    redes_notdone_today = _count_by_status(rows, today_text, redes_codes, "notdone")
    b2c_notdone_last_week = _count_by_status(
        rows,
        last_week_text,
        b2c_codes,
        "notdone",
        comparison_until_time,
    )
    redes_notdone_last_week = _count_by_status(
        rows,
        last_week_text,
        redes_codes,
        "notdone",
        comparison_until_time,
    )

    last_7_days = []
    for day_offset in range(7):
        current = last_7_from + timedelta(days=day_offset)
        current_text = _date_text(current)

        last_7_days.append({
            "date": current_text,
            "b2c_completed": _count_completed(rows, current_text, b2c_codes),
            "redes_completed": _count_completed(rows, current_text, redes_codes),
            "notdone": int(all_notdone_by_day.get(current_text, 0)),
        })

    top_cities = list(city_stats.values())
    for item in top_cities:
        item["completion_rate"] = _completion_rate(item["total"], item["completed"])
    top_cities.sort(key=lambda item: item["total"], reverse=True)

    b2c_type_rows = [
        {
            "code": code,
            "label": labels.get(code, code),
            "total": total,
        }
        for code, total in b2c_by_type_today.items()
    ]
    b2c_type_rows.sort(key=lambda item: item["total"], reverse=True)

    redes_type_rows = [
        {
            "code": code,
            "label": labels.get(code, code),
            "total": total,
        }
        for code, total in redes_by_type_today.items()
    ]
    redes_type_rows.sort(key=lambda item: item["total"], reverse=True)

    activity_options = [
        {
            "code": code,
            "label": labels.get(code, code),
            "group": "redes" if code in redes_codes else "b2c",
        }
        for code in sorted(b2c_codes.union(redes_codes), key=lambda item: labels.get(item, item))
    ]

    return {
        "generated_at": _dt_text(_now()),
        "periods": {
            "today": today_text,
            "same_weekday_last_week": last_week_text,
            "comparison_until_time": comparison_until_text,
            "last_7_days_from": _date_text(last_7_from),
            "last_7_days_to": today_text,
        },
        "kpis": {
            "b2c_completed_today": b2c_completed_today,
            "redes_completed_today": redes_completed_today,
            "b2c_completed_last_week_same_day": b2c_completed_last_week,
            "redes_completed_last_week_same_day": redes_completed_last_week,
            "b2c_variation_percent": _variation_percent(b2c_completed_today, b2c_completed_last_week),
            "redes_variation_percent": _variation_percent(redes_completed_today, redes_completed_last_week),
            "b2c_notdone_today": b2c_notdone_today,
            "redes_notdone_today": redes_notdone_today,
            "b2c_notdone_last_week_same_day": b2c_notdone_last_week,
            "redes_notdone_last_week_same_day": redes_notdone_last_week,
            "b2c_notdone_variation_percent": _variation_percent(b2c_notdone_today, b2c_notdone_last_week),
            "redes_notdone_variation_percent": _variation_percent(redes_notdone_today, redes_notdone_last_week),
        },
        "today_by_status": _list_from_counter(today_by_status, "status"),
        "b2c_by_type_today": b2c_type_rows,
        "redes_by_type_today": redes_type_rows,
        "last_7_days": last_7_days,
        "top_cities": top_cities[:10],
        "activity_options": activity_options,
        "dashboard_rows": dashboard_rows,
    }


def refresh_dashboard_snapshot():
    try:
        _update_progress(3, "Carregando tipos de atividade")
        activity_maps = _load_activity_type_maps()
        activity_codes = sorted(activity_maps["b2c_codes"].union(activity_maps["redes_codes"]))

        today = _today()
        date_from = _date_text(today - timedelta(days=7))
        date_to = _date_text(today)

        _update_progress(8, "Iniciando consulta no OFS")
        rows = _fetch_dashboard_activities(date_from, date_to, activity_codes)

        _update_progress(90, "Montando indicadores do dashboard")
        payload = _build_payload(rows, activity_maps)

        _update_progress(98, "Salvando snapshot do dashboard")
        _finish_success(payload)

    except Exception as exc:
        _finish_failure(str(exc))

def _start_background_refresh():
    thread = threading.Thread(target=refresh_dashboard_snapshot, daemon=True)
    thread.start()


def get_or_start_dashboard_snapshot():
    snapshot = _load_snapshot()

    if _snapshot_is_valid(snapshot):
        return _serialize_snapshot(snapshot)

    if snapshot and snapshot.get("status") == "running" and not _running_is_stale(snapshot):
        return _serialize_snapshot(snapshot)

    if _try_mark_running():
        _start_background_refresh()

    snapshot = _load_snapshot()
    return _serialize_snapshot(snapshot)


def _serialize_snapshot(snapshot):
    snapshot = snapshot or {}

    return {
        "status": snapshot.get("status") or "running",
        "payload": snapshot.get("payload") or {},
        "error_text": snapshot.get("error_text"),
        "updated_at": _dt_text(snapshot.get("updated_at")),
        "expires_at": _dt_text(snapshot.get("expires_at")),
        "started_at": _dt_text(snapshot.get("started_at")),
        "finished_at": _dt_text(snapshot.get("finished_at")),
        "has_payload": bool(snapshot.get("payload")),
        "progress_percent": snapshot.get("progress_percent"),
        "progress_message": snapshot.get("progress_message"),
        "progress_updated_at": _dt_text(snapshot.get("progress_updated_at")),
    }


def get_dashboard_snapshot_status():
    snapshot = _load_snapshot()
    serialized = _serialize_snapshot(snapshot)

    return {
        "status": serialized["status"],
        "updated_at": serialized["updated_at"],
        "expires_at": serialized["expires_at"],
        "started_at": serialized["started_at"],
        "finished_at": serialized["finished_at"],
        "has_payload": serialized["has_payload"],
        "error_text": serialized["error_text"],
        "progress_percent": serialized["progress_percent"],
        "progress_message": serialized["progress_message"],
        "progress_updated_at": serialized["progress_updated_at"],
    }
def unlock_dashboard_snapshot() -> dict:
    now = _now()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE dashboard_operacional_snapshot
            SET
                status = 'completed',
                expires_at = %s,
                started_at = NULL,
                progress_percent = 0,
                progress_message = 'Atualização destravada manualmente',
                progress_updated_at = %s
            WHERE snapshot_key = %s
            """,
            (
                now - timedelta(minutes=1),
                now,
                SNAPSHOT_KEY,
            ),
        )
        conn.commit()

        return {
            "ok": True,
            "message": "Atualização destravada. Abra o dashboard novamente para iniciar uma nova atualização.",
        }

    finally:
        cur.close()
        conn.close()