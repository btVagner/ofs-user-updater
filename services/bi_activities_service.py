import json
import uuid
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import requests

from database.connection import get_connection
from ofs.client import OFSClient


API_LIMIT = 200

FIELD_CITY = "city"

CLOSE_REASON_FIELDS = [
    "XA_SER_CLO_PRO_ADA",
    "XA_SER_CLO_IMP_ADA",
    "XA_SER_CLO_PRO_NG",
    "XA_SER_CLO_INP_NG",
]

DEFAULT_CLOSE_REASON = "Sem motivo de fechamento informado"

BASE_FIELDS = [
    "activityId",
    "apptNumber",
    "activityType",
    "status",
    "resourceId",
    FIELD_CITY,
    *CLOSE_REASON_FIELDS,
]


class BIActivitiesError(Exception):
    pass


def _normalize_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise BIActivitiesError(f"Data inválida: {value!r}")


def _ensure_valid_period(date_from, date_to) -> Tuple[date, date]:
    df = _normalize_date(date_from)
    dt = _normalize_date(date_to)

    if df > dt:
        raise BIActivitiesError("date_from não pode ser maior que date_to.")

    if (dt - df).days > 31:
        raise BIActivitiesError("O intervalo máximo permitido é de 31 dias.")

    return df, dt


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def _create_job(
    job_id: str,
    date_from: date,
    date_to: date,
    statuses: Optional[List[str]],
    created_by_user_id=None,
    created_by_username=None,
) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO ofs_activities_bi_jobs (
                job_id,
                started_at,
                status,
                date_from,
                date_to,
                statuses_json,
                activity_types_json,
                created_by_user_id,
                created_by_username
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                datetime.now(),
                "running",
                date_from,
                date_to,
                _json_dumps(statuses or []),
                _json_dumps([]),
                created_by_user_id,
                created_by_username,
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def _update_job_activity_types(job_id: str, activity_types: List[str]) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE ofs_activities_bi_jobs
            SET activity_types_json = %s
            WHERE job_id = %s
            """,
            (
                _json_dumps(activity_types),
                job_id,
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def _finish_job_success(
    job_id: str,
    total_fetched: int,
    total_inserted: int,
) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE ofs_activities_bi_jobs
            SET
                finished_at = %s,
                status = %s,
                total_fetched = %s,
                total_inserted = %s,
                error_text = NULL
            WHERE job_id = %s
            """,
            (
                datetime.now(),
                "success",
                total_fetched,
                total_inserted,
                job_id,
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def _finish_job_error(job_id: str, error_text: str) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE ofs_activities_bi_jobs
            SET
                finished_at = %s,
                status = %s,
                error_text = %s
            WHERE job_id = %s
            """,
            (
                datetime.now(),
                "error",
                error_text[:60000] if error_text else None,
                job_id,
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def list_recent_jobs(limit: int = 20) -> List[Dict]:
    safe_limit = max(1, min(int(limit or 20), 100))

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            f"""
            SELECT
                id,
                job_id,
                started_at,
                finished_at,
                status,
                date_from,
                date_to,
                statuses_json,
                activity_types_json,
                total_fetched,
                total_inserted,
                error_text,
                created_by_user_id,
                created_by_username,
                created_at
            FROM ofs_activities_bi_jobs
            ORDER BY id DESC
            LIMIT {safe_limit}
            """
        )
        return cursor.fetchall() or []
    finally:
        cursor.close()
        conn.close()


def _build_activity_type_map() -> Dict[str, Dict]:
    """
    Espera que a tabela ofs_activity_type_map tenha:
      - code
      - label_pt
      - is_active
      - category
      - include_in_bi
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                code,
                label_pt,
                is_active,
                category,
                include_in_bi
            FROM ofs_activity_type_map
            WHERE is_active = 1
            """
        )
        rows = cursor.fetchall() or []
    finally:
        cursor.close()
        conn.close()

    result = {}
    for row in rows:
        code = (row.get("code") or "").strip()
        if not code:
            continue
        result[code] = row
    return result


def _list_allowed_activity_types() -> List[str]:
    type_map = _build_activity_type_map()

    allowed_codes = []
    for code, row in type_map.items():
        category = (row.get("category") or "").strip().lower()
        include_in_bi = int(row.get("include_in_bi") or 0)

        if include_in_bi == 1 and category == "customer_home":
            allowed_codes.append(code)

    return sorted(set(allowed_codes))

def list_available_activity_types() -> List[Dict]:
    """
    Lista os tipos de atividade disponíveis para seleção na tela BI.
    Apenas tipos ativos, customer_home e include_in_bi = 1.
    """
    type_map = _build_activity_type_map()

    result = []
    for code, row in type_map.items():
        category = (row.get("category") or "").strip().lower()
        include_in_bi = int(row.get("include_in_bi") or 0)

        if include_in_bi == 1 and category == "customer_home":
            result.append({
                "code": code,
                "label": row.get("label_pt") or code,
            })

    return sorted(result, key=lambda item: item["label"])


def _resolve_activity_types_for_collection(
    selected_activity_types: Optional[List[str]] = None,
) -> List[str]:
    """
    Resolve os tipos que serão usados na coleta.

    Se selected_activity_types vier preenchido:
      - usa somente os selecionados
      - valida se todos existem entre os permitidos

    Se vier vazio/None:
      - usa todos os permitidos
    """
    allowed_codes = set(_list_allowed_activity_types())

    if not allowed_codes:
        raise BIActivitiesError("Nenhum tipo de atividade permitido para coleta BI.")

    if selected_activity_types:
        cleaned = []
        for code in selected_activity_types:
            value = (code or "").strip()
            if value:
                cleaned.append(value)

        selected_set = set(cleaned)

        invalid = sorted(selected_set - allowed_codes)
        if invalid:
            raise BIActivitiesError(
                "Tipos de atividade inválidos para coleta BI: "
                + ", ".join(invalid)
            )

        resolved = sorted(selected_set)
    else:
        resolved = sorted(allowed_codes)

    if not resolved:
        raise BIActivitiesError("Selecione pelo menos um tipo de atividade para coleta BI.")

    return resolved

def _load_resource_name_map() -> Dict[str, str]:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT resource_id, name
            FROM relatorios_ofs_resources
            WHERE status = 'active'
            """
        )
        rows = cursor.fetchall() or []
    finally:
        cursor.close()
        conn.close()

    result = {}
    for row in rows:
        resource_id = str(row.get("resource_id") or "").strip()
        name = (row.get("name") or "").strip()
        if resource_id:
            result[resource_id] = name
    return result


def _build_type_label_map() -> Dict[str, str]:
    type_map = _build_activity_type_map()
    return {
        code: (row.get("label_pt") or code)
        for code, row in type_map.items()
    }


def _build_query(statuses: Optional[List[str]], activity_types: List[str]) -> str:
    """
    Regra já validada no projeto:
    usar UM único q combinando filtros.
    """
    if not activity_types:
        raise BIActivitiesError("Nenhum tipo de atividade permitido para coleta BI.")

    activity_type_query = " OR ".join(
        [f"activityType=='{code}'" for code in activity_types]
    )
    activity_type_query = f"({activity_type_query})"

    if statuses:
        clean_statuses = [s.strip() for s in statuses if s and s.strip()]
        if clean_statuses:
            status_query = " OR ".join(
                [f"status=='{status}'" for status in clean_statuses]
            )
            status_query = f"({status_query})"
            return f"{status_query} and {activity_type_query}"

    return activity_type_query


def _build_fields_param() -> str:
    return ",".join(BASE_FIELDS)


def _extract_http_error_detail(response: requests.Response) -> str:
    text = ""
    try:
        text = response.text or ""
    except Exception:
        text = ""

    return (
        f"HTTP {response.status_code} ao consultar OFS.\n"
        f"URL: {response.url}\n"
        f"Resposta: {text[:5000]}"
    )


def _fetch_activities_page(
    client: OFSClient,
    date_from: date,
    date_to: date,
    q: str,
    offset: int,
    resources: str,
) -> Dict:
    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d"),
        "resources": resources,
        "q": q,
        "fields": _build_fields_param(),
        "limit": API_LIMIT,
        "offset": offset,
    }

    url = f"{client.base_url}/activities/"

    response = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json"},
        auth=client.auth,
        timeout=120,
    )

    if not response.ok:
        raise BIActivitiesError(_extract_http_error_detail(response))

    return response.json()


def _fetch_activities_with_types(
    date_from: date,
    date_to: date,
    statuses: Optional[List[str]],
    allowed_activity_types: List[str],
    resources: str,
) -> List[Dict]:
    q = _build_query(statuses=statuses, activity_types=allowed_activity_types)

    client = OFSClient()

    items: List[Dict] = []
    offset = 0

    while True:
        payload = _fetch_activities_page(client, date_from, date_to, q, offset, resources)

        page_items = payload.get("items") or []
        has_more = bool(payload.get("hasMore"))

        items.extend(page_items)

        if not has_more or not page_items:
            break

        offset += len(page_items)

    return items


def fetch_activities(
    date_from,
    date_to,
    statuses: Optional[List[str]] = None,
    resources: str = "02",
    activity_types: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Busca atividades da OFS respeitando:
    - intervalo máximo de 31 dias
    - um único parâmetro q
    - paginação por quantidade real retornada
    """
    df, dt = _ensure_valid_period(date_from, date_to)
    allowed_activity_types = _resolve_activity_types_for_collection(activity_types)

    return _fetch_activities_with_types(
        date_from=df,
        date_to=dt,
        statuses=statuses,
        allowed_activity_types=allowed_activity_types,
        resources=resources,
    )


def _safe_str(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

def _resolve_close_reason(activity: Dict) -> str:
    for field_name in CLOSE_REASON_FIELDS:
        value = _safe_str(activity.get(field_name))
        if value:
            return value

    return DEFAULT_CLOSE_REASON

def _prepare_snapshot_rows(
    activities: List[Dict],
    collected_at: datetime,
    snapshot_date: date,
    job_id: str,
) -> List[Tuple]:
    type_label_map = _build_type_label_map()
    resource_name_map = _load_resource_name_map()

    rows = []

    for activity in activities:
        activity_id = activity.get("activityId")
        if not activity_id:
            continue

        activity_type_code = _safe_str(activity.get("activityType"))
        activity_type_label = type_label_map.get(
            activity_type_code or "",
            activity_type_code or "Sem tipo",
        )

        resource_id = _safe_str(activity.get("resourceId"))
        resource_name = resource_name_map.get(
            resource_id or "",
            "Técnico não encontrado na base",
        )
        row = (
            job_id,
            collected_at,
            snapshot_date,
            int(activity_id),
            _safe_str(activity.get("apptNumber")),
            _safe_str(activity.get("status")),
            activity_type_code,
            activity_type_label,
            resource_id,
            resource_name,
            _resolve_close_reason(activity),
            _safe_str(activity.get(FIELD_CITY)),
            1,
        )
        rows.append(row)

    return rows


def insert_snapshot_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO ofs_activities_bi_snapshot (
                job_id,
                collected_at,
                snapshot_date,
                activity_id,
                appt_number,
                status,
                activity_type_code,
                activity_type_label,
                resource_id,
                resource_name,
                close_reason,
                city,
                is_customer_home
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
        conn.commit()
        return cursor.rowcount or 0
    finally:
        cursor.close()
        conn.close()


def run_collection(
    date_from,
    date_to,
    statuses: Optional[List[str]] = None,
    resources: str = "02",
    activity_types: Optional[List[str]] = None,
    created_by_user_id=None,
    created_by_username=None,
) -> Dict:
    """
    Executa uma coleta completa, salva no banco e registra o job.
    """
    df, dt = _ensure_valid_period(date_from, date_to)

    job_id = str(uuid.uuid4())

    _create_job(
        job_id=job_id,
        date_from=df,
        date_to=dt,
        statuses=statuses,
        created_by_user_id=created_by_user_id,
        created_by_username=created_by_username,
    )

    try:
        allowed_activity_types = _resolve_activity_types_for_collection(activity_types)
        _update_job_activity_types(job_id, allowed_activity_types)

        collected_at = datetime.now()
        snapshot_date = collected_at.date()

        activities = _fetch_activities_with_types(
            date_from=df,
            date_to=dt,
            statuses=statuses,
            allowed_activity_types=allowed_activity_types,
            resources=resources,
        )
        rows = _prepare_snapshot_rows(
            activities=activities,
            collected_at=collected_at,
            snapshot_date=snapshot_date,
            job_id=job_id,
        )
        inserted = insert_snapshot_rows(rows)

        _finish_job_success(
            job_id=job_id,
            total_fetched=len(activities),
            total_inserted=inserted,
        )

        return {
            "ok": True,
            "job_id": job_id,
            "date_from": df.strftime("%Y-%m-%d"),
            "date_to": dt.strftime("%Y-%m-%d"),
            "collected_at": collected_at.strftime("%Y-%m-%d %H:%M:%S"),
            "total_fetched": len(activities),
            "total_inserted": inserted,
            "activity_types": allowed_activity_types,
        }

    except Exception as exc:
        error_text = str(exc)
        _finish_job_error(job_id, error_text)

        if isinstance(exc, BIActivitiesError):
            raise

        raise BIActivitiesError(error_text) from exc


def get_last_job_summary() -> Dict:
    """
    Retorna um resumo operacional do último job BI.
    Usado pela tela /bi-activities para debug rápido.
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(
            """
            SELECT
                id,
                job_id,
                status,
                started_at,
                finished_at,
                date_from,
                date_to,
                total_fetched,
                total_inserted,
                activity_types_json,
                error_text
            FROM ofs_activities_bi_jobs
            ORDER BY id DESC
            LIMIT 1
            """
        )
        job = cursor.fetchone()

        if not job:
            return {
                "has_data": False,
                "message": "Nenhum job encontrado.",
            }

        job_id = job["job_id"]

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_snapshot,

                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS total_completed,
                SUM(CASE WHEN status = 'notdone' THEN 1 ELSE 0 END) AS total_notdone,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS total_pending,
                SUM(CASE WHEN status = 'started' THEN 1 ELSE 0 END) AS total_started,

                SUM(
                    CASE
                        WHEN status IN ('completed', 'notdone')
                         AND close_reason = 'Sem motivo de fechamento informado'
                        THEN 1 ELSE 0
                    END
                ) AS closed_without_close_reason,

                SUM(
                    CASE
                        WHEN resource_name = 'Técnico não encontrado na base'
                        THEN 1 ELSE 0
                    END
                ) AS resource_not_found,

                SUM(
                    CASE
                        WHEN city IS NULL OR city = ''
                        THEN 1 ELSE 0
                    END
                ) AS empty_city
            FROM ofs_activities_bi_snapshot
            WHERE job_id = %s
            """,
            (job_id,),
        )

        summary = cursor.fetchone() or {}

        def safe_int(value):
            return int(value or 0)

        return {
            "has_data": True,
            "job": {
                "id": job.get("id"),
                "job_id": job.get("job_id"),
                "status": job.get("status"),
                "started_at": str(job.get("started_at")) if job.get("started_at") else None,
                "finished_at": str(job.get("finished_at")) if job.get("finished_at") else None,
                "date_from": str(job.get("date_from")) if job.get("date_from") else None,
                "date_to": str(job.get("date_to")) if job.get("date_to") else None,
                "total_fetched": safe_int(job.get("total_fetched")),
                "total_inserted": safe_int(job.get("total_inserted")),
                "activity_types_json": job.get("activity_types_json"),
                "has_error": bool(job.get("error_text")),
            },
            "summary": {
                "total_snapshot": safe_int(summary.get("total_snapshot")),
                "total_completed": safe_int(summary.get("total_completed")),
                "total_notdone": safe_int(summary.get("total_notdone")),
                "total_pending": safe_int(summary.get("total_pending")),
                "total_started": safe_int(summary.get("total_started")),
                "closed_without_close_reason": safe_int(summary.get("closed_without_close_reason")),
                "resource_not_found": safe_int(summary.get("resource_not_found")),
                "empty_city": safe_int(summary.get("empty_city")),
            },
        }

    finally:
        cursor.close()
        conn.close()