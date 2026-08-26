import json
import os
import re
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
SUPPORT_ACTIVITY_CODES = {"SUP", "SUP_QUA", "SUP_REP"}
SUPPORT_ACTIVITY_FILTER_CODE = "SUPORTE"
SUPPORT_ACTIVITY_LABEL = "Suporte"
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


def _load_snapshot():
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


def _load_snapshot_status():
    """Lê somente metadata do snapshot, sem transferir/deserializar payload_json."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT
                snapshot_key,
                status,
                error_text,
                updated_at,
                expires_at,
                started_at,
                finished_at,
                progress_percent,
                progress_message,
                progress_updated_at,
                payload_json IS NOT NULL AS has_payload
            FROM dashboard_operacional_snapshot
            WHERE snapshot_key = %s
            """,
            (SNAPSHOT_KEY,),
        )
        return cur.fetchone()
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
        "apptNumber",
        "activityType",
        "status",
        "resourceId",
        "city",
        "date",
        "endTime",
        "XA_AV_CLI",
        "XA_AV_CLI_CAT",
        "XA_AV_CLI_SUB_CAT",
        "XA_AV_CLI_CON",
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


def _parse_clock_minutes(value):
    """Replica a leitura de horário usada historicamente pelo JS do dashboard."""
    text = str(value or "").strip()
    match = re.search(r"(?:^|\s|T)(\d{2}):(\d{2})(?::\d{2})?", text)
    if not match:
        return None

    hours = int(match.group(1))
    minutes = int(match.group(2))
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        return None

    return hours * 60 + minutes


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

def _clean_text(value, fallback="Não informado"):
    value = str(value or "").strip()
    return value if value else fallback


def _parse_customer_rating(value):
    value = str(value or "").strip()

    if not value:
        return None

    try:
        rating = int(float(value))
    except ValueError:
        return None

    if rating < 1 or rating > 5:
        return None

    return rating


def _is_customer_evaluation_completed(value):
    return str(value or "").strip() == "1"


def _build_customer_thermometer(rows, today_text):
    rating_distribution = {rating: 0 for rating in range(1, 6)}
    categories = defaultdict(lambda: {"category": "", "total": 0, "critical": 0})
    subcategories = defaultdict(lambda: {
        "category": "",
        "subcategory": "",
        "total": 0,
        "critical": 0,
    })

    total = 0
    rating_sum = 0
    satisfied = 0
    critical = 0
    critical_rows = []

    for item in rows:
        if str(item.get("date") or "").strip() != today_text:
            continue

        rating = _parse_customer_rating(item.get("XA_AV_CLI"))
        if rating is None:
            continue

        category = _clean_text(item.get("XA_AV_CLI_CAT"))
        subcategory = _clean_text(item.get("XA_AV_CLI_SUB_CAT"))

        total += 1
        rating_sum += rating
        rating_distribution[rating] += 1

        if rating >= 4:
            satisfied += 1
        else:
            critical += 1

        if rating <= 3:
            if category and category != "Não informado":
                categories[category]["category"] = category
                categories[category]["total"] += 1
                categories[category]["critical"] += 1

            if subcategory and subcategory != "Não informado":
                sub_key = (category, subcategory)
                subcategories[sub_key]["category"] = category
                subcategories[sub_key]["subcategory"] = subcategory
                subcategories[sub_key]["total"] += 1
                subcategories[sub_key]["critical"] += 1
            critical_rows.append({
                "apptNumber": _clean_text(item.get("apptNumber"), "-"),
                "rating": rating,
                "category": category,
                "subcategory": subcategory,
                "endTime": str(item.get("endTime") or "").strip(),
            })

    average_rating = round(rating_sum / total, 2) if total else 0
    satisfied_percent = round((satisfied / total) * 100, 1) if total else 0
    critical_percent = round((critical / total) * 100, 1) if total else 0

    category_rows = list(categories.values())
    category_rows.sort(key=lambda item: item["total"], reverse=True)

    subcategory_rows = list(subcategories.values())
    subcategory_rows.sort(key=lambda item: item["total"], reverse=True)

    critical_rows.sort(key=lambda item: (item["rating"], item["endTime"] or ""))

    return {
        "summary": {
            "total": total,
            "average_rating": average_rating,
            "satisfied": satisfied,
            "satisfied_percent": satisfied_percent,
            "critical": critical,
            "critical_percent": critical_percent,
        },
        "rating_distribution": [
            {
                "rating": rating,
                "total": rating_distribution.get(rating, 0),
            }
            for rating in range(1, 6)
        ],
        "categories": category_rows[:8],
        "subcategories": subcategory_rows[:10],
        "critical_rows": critical_rows[:12],
    }
def _dashboard_type_filter_code(activity_type):
    activity_type = str(activity_type or "").strip()

    if activity_type in SUPPORT_ACTIVITY_CODES:
        return SUPPORT_ACTIVITY_FILTER_CODE

    return activity_type


def _dashboard_type_label(activity_type, labels):
    activity_type = str(activity_type or "").strip()

    if activity_type in SUPPORT_ACTIVITY_CODES:
        return SUPPORT_ACTIVITY_LABEL

    return labels.get(activity_type, activity_type or "Não informado")


def _new_filter_type_metrics():
    return {
        "status_today": defaultdict(int),
        "evolution": defaultdict(lambda: {"completed": 0, "notdone": 0}),
        "evolution_until": defaultdict(lambda: {"completed": 0, "notdone": 0}),
        "cities_today": {},
    }


def _accumulate_filter_metrics(
    type_metrics,
    filter_code,
    item_date,
    status,
    city,
    end_time,
    *,
    today_text,
    date_from,
    date_to,
    comparison_until_time,
):
    filter_code = str(filter_code or "").strip()
    if not filter_code:
        return

    metrics = type_metrics.get(filter_code)
    if metrics is None:
        metrics = _new_filter_type_metrics()
        type_metrics[filter_code] = metrics

    item_date = str(item_date or "").strip()
    status = str(status or "nao_informado").strip().lower() or "nao_informado"
    city = str(city or "Não informado").strip() or "Não informado"

    if item_date == today_text:
        metrics["status_today"][status] += 1

        city_row = metrics["cities_today"].get(city)
        if city_row is None:
            city_row = {
                "city": city,
                "total": 0,
                "completed": 0,
                "notdone": 0,
            }
            metrics["cities_today"][city] = city_row

        city_row["total"] += 1
        if status == "completed":
            city_row["completed"] += 1
        if status == "notdone":
            city_row["notdone"] += 1

    if not date_from or not date_to or not (date_from <= item_date <= date_to):
        return

    if status not in {"completed", "notdone"}:
        return

    metrics["evolution"][item_date][status] += 1

    if comparison_until_time is None:
        return

    end_minutes = _parse_clock_minutes(end_time)
    until_minutes = comparison_until_time.hour * 60 + comparison_until_time.minute
    if end_minutes is not None and end_minutes <= until_minutes:
        metrics["evolution_until"][item_date][status] += 1


def _finalize_filter_read_model(type_metrics):
    compact_types = {}
    for filter_code, metrics in type_metrics.items():
        compact_types[filter_code] = {
            "status_today": dict(sorted(metrics["status_today"].items())),
            "evolution": {
                item_date: values
                for item_date, values in sorted(metrics["evolution"].items())
                if values.get("completed") or values.get("notdone")
            },
            "evolution_until": {
                item_date: values
                for item_date, values in sorted(metrics["evolution_until"].items())
                if values.get("completed") or values.get("notdone")
            },
            "cities_today": sorted(
                metrics["cities_today"].values(),
                key=lambda item: item["city"],
            ),
        }

    return {"types": compact_types}


def _build_filter_read_model(dashboard_rows, periods):
    """Converte snapshots legados em agregados sem expor atividades ao browser."""
    today_text = str(periods.get("today") or "")
    date_from = str(periods.get("last_7_days_from") or "")
    date_to = str(periods.get("last_7_days_to") or "")

    comparison_until_time = None
    comparison_until_text = str(periods.get("comparison_until_time") or "").strip()
    if comparison_until_text:
        try:
            comparison_until_time = datetime.strptime(comparison_until_text, "%H:%M").time()
        except ValueError:
            comparison_until_time = None

    type_metrics = {}
    for row in dashboard_rows:
        _accumulate_filter_metrics(
            type_metrics,
            row.get("activityTypeFilterCode") or row.get("activityType"),
            row.get("date"),
            row.get("status"),
            row.get("city"),
            row.get("endTime"),
            today_text=today_text,
            date_from=date_from,
            date_to=date_to,
            comparison_until_time=comparison_until_time,
        )

    return _finalize_filter_read_model(type_metrics)

def _build_payload(rows, activity_maps):
    now = _now()
    today = now.date()
    comparison_until_time = now.time().replace(microsecond=0)
    comparison_until_text = now.strftime("%H:%M")
    last_week_same_day = today - timedelta(days=7)
    last_7_from = today - timedelta(days=6)

    today_text = _date_text(today)
    last_week_text = _date_text(last_week_same_day)
    periods = {
        "today": today_text,
        "same_weekday_last_week": last_week_text,
        "comparison_until_time": comparison_until_text,
        "last_7_days_from": _date_text(last_7_from),
        "last_7_days_to": today_text,
    }

    labels = activity_maps["labels"]
    b2c_codes = activity_maps["b2c_codes"]
    redes_codes = activity_maps["redes_codes"]

    today_by_status = defaultdict(int)
    b2c_by_type_today = defaultdict(int)
    redes_by_type_today = defaultdict(int)
    city_stats = {}
    all_notdone_by_day = defaultdict(int)
    filter_type_metrics = {}

    for item in rows:
        item_date = str(item.get("date") or "").strip()
        status = str(item.get("status") or "nao_informado").strip().lower() or "nao_informado"
        activity_type = str(item.get("activityType") or "").strip()
        activity_type_filter_code = _dashboard_type_filter_code(activity_type)
        city = str(item.get("city") or "Não informado").strip() or "Não informado"

        _accumulate_filter_metrics(
            filter_type_metrics,
            activity_type_filter_code,
            item_date,
            status,
            city,
            item.get("endTime"),
            today_text=today_text,
            date_from=periods["last_7_days_from"],
            date_to=periods["last_7_days_to"],
            comparison_until_time=comparison_until_time,
        )

        if status == "notdone":
            all_notdone_by_day[item_date] += 1

        if item_date != today_text:
            continue

        today_by_status[status] += 1

        if activity_type in b2c_codes:
            b2c_by_type_today[activity_type_filter_code] += 1

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
            "label": SUPPORT_ACTIVITY_LABEL if code == SUPPORT_ACTIVITY_FILTER_CODE else labels.get(code, code),
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

    activity_option_map = {}

    for code in b2c_codes.union(redes_codes):
        option_code = _dashboard_type_filter_code(code)
        option_label = _dashboard_type_label(code, labels)

        if option_code not in activity_option_map:
            activity_option_map[option_code] = {
                "code": option_code,
                "label": option_label,
                "group": "redes" if code in redes_codes else "b2c",
            }

    activity_options = sorted(
        activity_option_map.values(),
        key=lambda item: item["label"],
    )

    filter_read_model = _finalize_filter_read_model(filter_type_metrics)

    return {
        "generated_at": _dt_text(_now()),
        "periods": periods,
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
        "filter_read_model": filter_read_model,
        "customer_thermometer": _build_customer_thermometer(rows, today_text),
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
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        has_legacy_rows = isinstance(payload, dict) and isinstance(payload.get("dashboard_rows"), list)
        if has_legacy_rows and _try_mark_running():
            _start_background_refresh()
            snapshot = dict(snapshot)
            snapshot["status"] = "running"
            snapshot["started_at"] = _now()
        return _serialize_snapshot(snapshot)

    if snapshot and snapshot.get("status") == "running" and not _running_is_stale(snapshot):
        return _serialize_snapshot(snapshot)

    if _try_mark_running():
        _start_background_refresh()

    snapshot = _load_snapshot()
    return _serialize_snapshot(snapshot)


def _prepare_payload_for_browser(payload):
    """Garante o contrato compacto mesmo durante a transição de snapshots antigos."""
    if not isinstance(payload, dict):
        return {}

    dashboard_rows = payload.get("dashboard_rows")
    if not isinstance(dashboard_rows, list):
        return payload

    compact_payload = dict(payload)
    if not isinstance(compact_payload.get("filter_read_model"), dict):
        compact_payload["filter_read_model"] = _build_filter_read_model(
            dashboard_rows,
            compact_payload.get("periods") or {},
        )
    compact_payload.pop("dashboard_rows", None)
    return compact_payload


def _serialize_snapshot(snapshot):
    snapshot = snapshot or {}
    raw_payload = snapshot.get("payload") or {}
    browser_payload = _prepare_payload_for_browser(raw_payload)

    return {
        "status": snapshot.get("status") or "running",
        "payload": browser_payload,
        "error_text": snapshot.get("error_text"),
        "updated_at": _dt_text(snapshot.get("updated_at")),
        "expires_at": _dt_text(snapshot.get("expires_at")),
        "started_at": _dt_text(snapshot.get("started_at")),
        "finished_at": _dt_text(snapshot.get("finished_at")),
        "has_payload": bool(raw_payload),
        "progress_percent": snapshot.get("progress_percent"),
        "progress_message": snapshot.get("progress_message"),
        "progress_updated_at": _dt_text(snapshot.get("progress_updated_at")),
    }


def get_dashboard_snapshot_status():
    snapshot = _load_snapshot_status() or {}

    return {
        "status": snapshot.get("status") or "running",
        "updated_at": _dt_text(snapshot.get("updated_at")),
        "expires_at": _dt_text(snapshot.get("expires_at")),
        "started_at": _dt_text(snapshot.get("started_at")),
        "finished_at": _dt_text(snapshot.get("finished_at")),
        "has_payload": bool(snapshot.get("has_payload")),
        "error_text": snapshot.get("error_text"),
        "progress_percent": snapshot.get("progress_percent"),
        "progress_message": snapshot.get("progress_message"),
        "progress_updated_at": _dt_text(snapshot.get("progress_updated_at")),
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