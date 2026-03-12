import os
import time
import requests
from datetime import date
from requests.auth import HTTPBasicAuth

from database.connection import get_connection


API_URL = os.getenv("OIC_REPROCESS_URL", "").strip()

TOKEN_URL = os.getenv("OIC_TOKEN_URL", "").strip()
USERNAME = os.getenv("OIC_USERNAME", "").strip()
PASSWORD = os.getenv("OIC_PASSWORD", "").strip()
SCOPE = os.getenv("OIC_SCOPE", "").strip()
ASSERTION = os.getenv("OIC_ASSERTION", "").strip()

GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
TIMEOUT = 30
DELAY_SECONDS = 1.5

EXCLUDED_ACTIVITY_TYPES = (
    "LUNCH",
    "ALM",
    "MAN_VEIC",
    "EQP_DUP",
    "REUNIAO",
    "VIST_INV_TEC",
    "MAN",
    "EM ATIVIDADE B2B",
)


def _validate_env():
    missing = []

    if not API_URL:
        missing.append("OIC_REPROCESS_URL")
    if not TOKEN_URL:
        missing.append("OIC_TOKEN_URL")
    if not USERNAME:
        missing.append("OIC_USERNAME")
    if not PASSWORD:
        missing.append("OIC_PASSWORD")
    if not SCOPE:
        missing.append("OIC_SCOPE")
    if not ASSERTION:
        missing.append("OIC_ASSERTION")

    if missing:
        raise Exception(
            "Variáveis de ambiente ausentes: " + ", ".join(missing)
        )


def get_token():
    _validate_env()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    data = {
        "scope": SCOPE,
        "assertion": ASSERTION,
        "grant_type": GRANT_TYPE,
    }

    response = requests.post(
        TOKEN_URL,
        data=data,
        headers=headers,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=TIMEOUT,
    )

    if response.status_code != 200:
        raise Exception(
            f"Erro ao obter token: {response.status_code} - {response.text}"
        )

    token = response.json().get("access_token")

    if not token:
        raise Exception("Token não encontrado na resposta.")

    return token


def send_event(token, activity_id, event_type):
    today = date.today().isoformat()

    body = {
        "date": today,
        "activityId": str(activity_id),
        "eventType": event_type,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    return requests.post(
        API_URL,
        json=body,
        headers=headers,
        timeout=TIMEOUT,
    )


def _build_status_filter(statuses):
    statuses = statuses or []
    valid = {"completed", "notdone"}
    statuses = [s for s in statuses if s in valid]

    if not statuses:
        raise Exception("Selecione ao menos um status: completed e/ou notdone.")

    return statuses


def fetch_reprocessing_targets(date_from, date_to, activity_types, statuses):
    statuses = _build_status_filter(statuses)

    if not date_from or not date_to:
        raise Exception("Informe date_from e date_to.")

    if not activity_types:
        raise Exception("Selecione ao menos um activity_type.")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        placeholders_types = ", ".join(["%s"] * len(activity_types))
        placeholders_status = ", ".join(["%s"] * len(statuses))
        placeholders_excluded = ", ".join(["%s"] * len(EXCLUDED_ACTIVITY_TYPES))

        sql = f"""
            SELECT DISTINCT
                e.activity_id,
                e.activity_type,
                e.status,
                e.`date`
            FROM ofs_activities_errors e
            INNER JOIN ofs_pending_close_ng ng
                ON e.appt_number_norm = ng.numero_ose_norm
            WHERE e.`date` BETWEEN %s AND %s
              AND COALESCE(e.activity_type, '') IN ({placeholders_types})
              AND LOWER(COALESCE(e.status, '')) IN ({placeholders_status})
              AND (
                    NULLIF(TRIM(e.ng_dispatch_message), '') IS NOT NULL
                    OR NULLIF(TRIM(e.ng_response_message), '') IS NOT NULL
                  )
              AND COALESCE(e.activity_type, '') NOT IN ({placeholders_excluded})
              AND e.activity_id IS NOT NULL
              AND TRIM(e.activity_id) <> ''
            ORDER BY e.`date` ASC, e.activity_type ASC, e.activity_id ASC
        """

        params = (
            [date_from, date_to]
            + activity_types
            + statuses
            + list(EXCLUDED_ACTIVITY_TYPES)
        )

        cur.execute(sql, params)
        rows = cur.fetchall()

        return rows

    finally:
        cur.close()
        conn.close()


def map_status_to_event_type(status_value):
    value = str(status_value or "").strip().lower()

    if value == "completed":
        return "activityCompleted"

    if value == "notdone":
        return "activityNotDone"

    raise Exception(f"Status inválido para reprocessamento: {status_value}")


def insert_reprocess_log(cur, job_id, activity_id, event_type, status_code, response_text):
    cur.execute(
        """
        INSERT INTO ofs_reprocess_logs (
            job_id,
            activity_id,
            event_type,
            status_code,
            response_text
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            job_id,
            str(activity_id),
            str(event_type),
            status_code,
            (response_text or "")[:10000],
        )
    )


def update_job_progress(cur, job_id, progress, message, status="running"):
    cur.execute(
        """
        UPDATE ofs_import_jobs
        SET status = %s,
            progress = %s,
            message = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
          AND module = 'ofs.reprocessing'
        """,
        (status, progress, message[:1000], job_id)
    )


def is_cancel_requested(cur, job_id):
    cur.execute(
        """
        SELECT cancel_requested
        FROM ofs_import_jobs
        WHERE id = %s
          AND module = 'ofs.reprocessing'
        LIMIT 1
        """,
        (job_id,)
    )
    row = cur.fetchone()
    if not row:
        return False
    return int(row[0] or 0) == 1


def run_reprocess_job(job_id, targets):
    conn = get_connection()
    cur = conn.cursor()

    try:
        total = len(targets)

        if total == 0:
            update_job_progress(
                cur,
                job_id,
                100,
                "Nenhum activityId encontrado para os filtros informados.",
                status="done"
            )
            conn.commit()
            return

        update_job_progress(
            cur,
            job_id,
            1,
            "Obtendo token...",
            status="running"
        )
        conn.commit()

        token = get_token()

        for index, item in enumerate(targets, start=1):
            if is_cancel_requested(cur, job_id):
                update_job_progress(
                    cur,
                    job_id,
                    int(((index - 1) / total) * 100) if total > 0 else 0,
                    "Processo cancelado pelo usuário.",
                    status="canceled"
                )
                conn.commit()
                return

            activity_id = str(item.get("activity_id") or "").strip()
            status_value = str(item.get("status") or "").strip().lower()

            if not activity_id:
                continue

            event_type = map_status_to_event_type(status_value)

            try:
                response = send_event(token, activity_id, event_type)

                # Se o token expirou, renova e tenta novamente 1 vez
                if response.status_code == 401:
                    token = get_token()
                    response = send_event(token, activity_id, event_type)

                insert_reprocess_log(
                    cur,
                    job_id,
                    activity_id,
                    event_type,
                    response.status_code,
                    response.text,
                )

                message = (
                    f"{index}/{total} | activityId={activity_id} | "
                    f"eventType={event_type} | status={response.status_code}"
                )

            except Exception as e:
                insert_reprocess_log(
                    cur,
                    job_id,
                    activity_id,
                    event_type,
                    0,
                    str(e),
                )

                message = (
                    f"{index}/{total} | activityId={activity_id} | "
                    f"eventType={event_type} | erro={str(e)}"
                )

            progress = int((index / total) * 100)

            update_job_progress(
                cur,
                job_id,
                progress,
                message,
                status="running"
            )
            conn.commit()

            if index < total:
                time.sleep(DELAY_SECONDS)

        update_job_progress(
            cur,
            job_id,
            100,
            "Finalizado com sucesso.",
            status="done"
        )
        conn.commit()

    except Exception as e:
        update_job_progress(
            cur,
            job_id,
            100,
            f"Erro no reprocessamento: {str(e)}",
            status="error"
        )
        conn.commit()

    finally:
        cur.close()
        conn.close()