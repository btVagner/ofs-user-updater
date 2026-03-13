import json
import os
import threading
import time
import uuid
from typing import Optional, List, Tuple

import requests

from database.connection import get_connection


OIC_DDC_URL = os.getenv("OIC_DDC_URL", "").strip()
OIC_TOKEN_URL = os.getenv("OIC_TOKEN_URL", "").strip()
OIC_USERNAME = os.getenv("OIC_USERNAME", "").strip()
OIC_PASSWORD = os.getenv("OIC_PASSWORD", "").strip()
OIC_SCOPE = os.getenv("OIC_SCOPE", "").strip()
OIC_ASSERTION = os.getenv("OIC_ASSERTION", "").strip()
OIC_GRANT_TYPE = os.getenv("OIC_GRANT_TYPE", "").strip()

REQUEST_TIMEOUT = 60
DEFAULT_EVENT = "activityCreated"
MASSIVE_JOB_LOCK_NAME = "ddc_mensageria_massivo_lock"

_token_cache = {
    "access_token": None,
    "expires_at": 0.0,
}
_token_lock = threading.Lock()


class DDCMensageriaError(Exception):
    pass


def _validate_env():
    required = {
        "OIC_DDC_URL": OIC_DDC_URL,
        "OIC_TOKEN_URL": OIC_TOKEN_URL,
        "OIC_USERNAME": OIC_USERNAME,
        "OIC_PASSWORD": OIC_PASSWORD,
        "OIC_SCOPE": OIC_SCOPE,
        "OIC_ASSERTION": OIC_ASSERTION,
        "OIC_GRANT_TYPE": OIC_GRANT_TYPE,
    }

    missing = [key for key, value in required.items() if not value]
    if missing:
        raise DDCMensageriaError(
            "Variáveis de ambiente ausentes: " + ", ".join(missing)
        )


def get_fixed_event_option():
    return {
        "value": DEFAULT_EVENT,
        "label": "Envio de atividade criada",
    }


def _extract_access_token(token_response: dict) -> str:
    token = (
        token_response.get("access_token")
        or token_response.get("token")
        or token_response.get("id_token")
    )
    if not token:
        raise DDCMensageriaError(
            "Não foi possível localizar access_token na resposta do token."
        )
    return token


def _extract_expires_in(token_response: dict) -> int:
    try:
        return int(token_response.get("expires_in", 300))
    except Exception:
        return 300


def _request_new_token() -> str:
    _validate_env()

    payload = {
        "scope": OIC_SCOPE,
        "assertion": OIC_ASSERTION,
        "grant_type": OIC_GRANT_TYPE,
    }

    try:
        response = requests.post(
            OIC_TOKEN_URL,
            data=payload,
            auth=(OIC_USERNAME, OIC_PASSWORD),
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        raise DDCMensageriaError(f"Falha ao obter token OIC: {str(e)}")

    if response.status_code >= 400:
        raise DDCMensageriaError(
            f"Erro ao obter token OIC ({response.status_code}): {response.text}"
        )

    try:
        data = response.json()
    except Exception:
        raise DDCMensageriaError(f"Resposta de token inválida: {response.text}")

    access_token = _extract_access_token(data)
    expires_in = _extract_expires_in(data)

    expires_at = time.time() + max(expires_in - 60, 60)

    with _token_lock:
        _token_cache["access_token"] = access_token
        _token_cache["expires_at"] = expires_at

    return access_token


def _get_valid_token(force_refresh: bool = False) -> str:
    with _token_lock:
        cached_token = _token_cache.get("access_token")
        expires_at = float(_token_cache.get("expires_at") or 0)

    if force_refresh or not cached_token or time.time() >= expires_at:
        return _request_new_token()

    return cached_token


def _send_ddc_request(activity_id: str) -> dict:
    _validate_env()

    payload = {
        "activity_id": str(activity_id).strip(),
        "event": DEFAULT_EVENT,
    }

    def _do_request(token: str):
        return requests.post(
            OIC_DDC_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )

    try:
        token = _get_valid_token(force_refresh=False)
        response = _do_request(token)

        if response.status_code in (401, 403):
            token = _get_valid_token(force_refresh=True)
            response = _do_request(token)

    except Exception as e:
        raise DDCMensageriaError(
            f"Falha na chamada da API DDC para activity_id={activity_id}: {str(e)}"
        )

    try:
        response_body = response.json()
    except Exception:
        response_body = response.text

    return {
        "success": 200 <= response.status_code < 300,
        "status_code": response.status_code,
        "response_body": response_body,
    }


def send_single_ddc(activity_id: str) -> dict:
    activity_id = str(activity_id or "").strip()

    if not activity_id:
        raise DDCMensageriaError("Informe o ID da OS.")

    result = _send_ddc_request(activity_id)

    if result["success"]:
        return {
            "success": True,
            "message": f"OS {activity_id} enviada com sucesso. API respondeu {result['status_code']}.",
            "status_code": result["status_code"],
            "response_body": result["response_body"],
        }

    return {
        "success": False,
        "message": f"OS {activity_id} não enviada. API respondeu {result['status_code']}.",
        "status_code": result["status_code"],
        "response_body": result["response_body"],
    }


def _json_dump(value):
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _create_job(usuario_id: Optional[int], ids: List[str]) -> Tuple[int, str]:
    job_uuid = uuid.uuid4().hex

    conn = get_connection()
    cur = conn.cursor()

    lock_acquired = False

    try:
        conn.start_transaction()

        cur.execute("SELECT GET_LOCK(%s, 5)", (MASSIVE_JOB_LOCK_NAME,))
        lock_row = cur.fetchone()

        lock_value = None
        if lock_row:
            if isinstance(lock_row, (tuple, list)):
                lock_value = lock_row[0]
            else:
                try:
                    lock_value = list(lock_row.values())[0]
                except Exception:
                    lock_value = None

        if lock_value != 1:
            raise DDCMensageriaError(
                "Não foi possível obter lock para iniciar o envio massivo. Tente novamente."
            )

        lock_acquired = True

        cur.execute(
            """
            SELECT id
            FROM ddc_mensageria_jobs
            WHERE status IN ('pending', 'running')
            LIMIT 1
            """
        )
        active_job = cur.fetchone()

        if active_job:
            raise DDCMensageriaError(
                "Já existe um envio massivo em andamento. Aguarde a finalização para iniciar outro."
            )

        cur.execute(
            """
            INSERT INTO ddc_mensageria_jobs
            (job_uuid, usuario_id, event_name, status, total, processed, success_count, error_count, percent)
            VALUES (%s, %s, %s, 'pending', %s, 0, 0, 0, 0)
            """,
            (job_uuid, usuario_id, DEFAULT_EVENT, len(ids)),
        )
        job_id = cur.lastrowid

        item_sql = """
            INSERT INTO ddc_mensageria_job_items
            (job_id, activity_id, item_order, status)
            VALUES (%s, %s, %s, 'pending')
        """
        for idx, activity_id in enumerate(ids, start=1):
            cur.execute(item_sql, (job_id, activity_id, idx))

        conn.commit()
        return job_id, job_uuid

    except Exception:
        conn.rollback()
        raise

    finally:
        if lock_acquired:
            try:
                cur.execute("SELECT RELEASE_LOCK(%s)", (MASSIVE_JOB_LOCK_NAME,))
                cur.fetchone()
            except Exception:
                pass
        cur.close()
        conn.close()


def _set_job_running(job_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ddc_mensageria_jobs
            SET status = 'running',
                started_at = NOW()
            WHERE id = %s
            """,
            (job_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _update_job_item(
    job_id: int,
    job_item_id: int,
    success: bool,
    status_code: int,
    response_body,
    message: str
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ddc_mensageria_job_items
            SET status = %s,
                status_code = %s,
                response_body = %s,
                message = %s,
                processed_at = NOW()
            WHERE id = %s
              AND job_id = %s
            """,
            (
                "success" if success else "error",
                status_code,
                _json_dump(response_body),
                message,
                job_item_id,
                job_id,
            ),
        )

        cur.execute(
            """
            UPDATE ddc_mensageria_jobs
            SET processed = processed + 1,
                success_count = success_count + %s,
                error_count = error_count + %s,
                percent = ROUND(((processed + 1) / total) * 100)
            WHERE id = %s
            """,
            (
                1 if success else 0,
                0 if success else 1,
                job_id,
            ),
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()


def _finish_job(job_id: int, status: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ddc_mensageria_jobs
            SET status = %s,
                finished_at = NOW(),
                percent = CASE
                    WHEN total > 0 THEN ROUND((processed / total) * 100)
                    ELSE 0
                END
            WHERE id = %s
            """,
            (status, job_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _mark_job_error(job_id: int, message: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ddc_mensageria_jobs
            SET status = 'error',
                finished_at = NOW()
            WHERE id = %s
            """,
            (job_id,),
        )

        cur.execute(
            """
            INSERT INTO ddc_mensageria_job_items
            (job_id, activity_id, item_order, status, status_code, response_body, message, processed_at)
            VALUES (%s, %s, %s, 'error', %s, %s, %s, NOW())
            """,
            (
                job_id,
                "SYSTEM",
                999999999,
                0,
                None,
                message,
            ),
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()


def _process_mass_job(job_id: int):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT id, activity_id, item_order
            FROM ddc_mensageria_job_items
            WHERE job_id = %s
            ORDER BY item_order ASC
            """,
            (job_id,),
        )
        items = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    _set_job_running(job_id)

    try:
        for idx, item in enumerate(items):
            job_item_id = int(item["id"])
            activity_id = str(item["activity_id"]).strip()

            result = _send_ddc_request(activity_id)

            if result["success"]:
                message = f"OS {activity_id} enviada com sucesso. API respondeu {result['status_code']}."
            else:
                message = f"OS {activity_id} não enviada. API respondeu {result['status_code']}."

            _update_job_item(
                job_id=job_id,
                job_item_id=job_item_id,
                success=result["success"],
                status_code=result["status_code"],
                response_body=result["response_body"],
                message=message,
            )

            if idx < len(items) - 1:
                time.sleep(1)

        _finish_job(job_id, "finished")

    except Exception as e:
        _mark_job_error(job_id, f"Falha no processamento do lote: {str(e)}")
        raise


def start_massive_job(usuario_id: Optional[int], ids: List[str]) -> dict:
    clean_ids = [str(x).strip() for x in (ids or []) if str(x).strip()]

    if not clean_ids:
        raise DDCMensageriaError("Nenhum ID válido foi informado para o envio massivo.")

    job_id, job_uuid = _create_job(usuario_id=usuario_id, ids=clean_ids)

    thread = threading.Thread(
        target=_process_mass_job,
        args=(job_id,),
        daemon=True,
    )
    thread.start()

    return {
        "success": True,
        "job_id": job_uuid,
        "total": len(clean_ids),
        "message": f"Job massivo iniciado com {len(clean_ids)} ID(s).",
    }


def get_job_status(job_uuid: str) -> dict:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            """
            SELECT
                id,
                job_uuid,
                status,
                event_name,
                total,
                processed,
                success_count,
                error_count,
                percent,
                created_at,
                started_at,
                finished_at
            FROM ddc_mensageria_jobs
            WHERE job_uuid = %s
            """,
            (job_uuid,),
        )
        job = cur.fetchone()

        if not job:
            raise DDCMensageriaError("Job não encontrado.")

        cur.execute(
            """
            SELECT
                activity_id,
                status,
                status_code,
                message,
                DATE_FORMAT(processed_at, '%%H:%%i:%%s') AS timestamp
            FROM ddc_mensageria_job_items
            WHERE job_id = %s
              AND status IN ('success', 'error')
            ORDER BY item_order DESC
            LIMIT 100
            """,
            (job["id"],),
        )
        logs = cur.fetchall() or []

        return {
            "job_id": job["job_uuid"],
            "status": job["status"],
            "event": job["event_name"],
            "total": job["total"],
            "processed": job["processed"],
            "success_count": job["success_count"],
            "error_count": job["error_count"],
            "percent": job["percent"],
            "logs": logs,
            "created_at": str(job["created_at"]) if job["created_at"] else None,
            "started_at": str(job["started_at"]) if job["started_at"] else None,
            "finished_at": str(job["finished_at"]) if job["finished_at"] else None,
        }
    finally:
        cur.close()
        conn.close()