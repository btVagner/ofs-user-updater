# ofs/cleanup.py
# -*- coding: utf-8 -*-
import os
import time
import base64
import requests

from datetime import datetime, timedelta
from typing import Optional, Dict, Iterable, List, Tuple
from dotenv import load_dotenv

load_dotenv()


BASE_URL = os.getenv(
    "OFS_BASE_URL",
    "https://verointernet.fs.ocs.oraclecloud.com/rest/ofscCore/v1"
).rstrip("/")

USERNAME = os.getenv("OFS_USERNAME", "XXXXX")
PASSWORD = os.getenv("OFS_PASSWORD", "XXXXX")

LIMIT = int(os.getenv("OFS_PAGE_LIMIT", "100"))
TIMEOUT = int(os.getenv("OFS_TIMEOUT", "30"))
CLEANUP_ACTION_TIMEOUT = int(os.getenv("OFS_CLEANUP_ACTION_TIMEOUT", "12"))
PAUSE = float(os.getenv("OFS_PAUSE", "0.2"))
VERIFY_SSL = os.getenv("OFS_VERIFY_SSL", "true").lower() not in ("0", "false", "no")


def parse_last_login(s: Optional[str]) -> Optional[datetime]:
    """
    Aceita ISO com T/Z e também 'YYYY-MM-DD HH:MM:SS'.
    Retorna None se vazio ou inválido.
    """
    if not s:
        return None

    s = str(s).strip()

    if s.lower() in ("none", "null", "0", "-", ""):
        return None

    s2 = s.replace("T", " ").replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(s2)
    except ValueError:
        pass

    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def older_than(dt: Optional[datetime], days: int) -> bool:
    """
    Sem lastLoginTime retorna True para manter compatibilidade.
    Quando only_logged_once=True, usuários sem lastLoginTime já são filtrados antes.
    """
    if dt is None:
        return True

    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    return dt <= (now - timedelta(days=days))


def get_session() -> requests.Session:
    session = requests.Session()

    token = base64.b64encode(
        f"{USERNAME}:{PASSWORD}".encode("utf-8")
    ).decode("ascii")

    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}",
        "User-Agent": "ofs-cleanup/flask",
    })

    session.verify = VERIFY_SSL

    return session


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    request_timeout: Optional[int] = None,
    **kwargs
) -> Tuple[bool, str, Optional[requests.Response], Optional[str]]:
    """
    Executa request com até 3 tentativas para 429/5xx.

    Retorno:
    - ok
    - code ou ERR:Exception
    - response
    - erro em texto
    """
    timeout = request_timeout or TIMEOUT

    for attempt in range(3):
        try:
            response = session.request(
                method,
                url,
                timeout=timeout,
                **kwargs
            )

            code = str(response.status_code)

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep((2 ** attempt) * PAUSE)
                continue

            return True, code, response, None

        except requests.RequestException as e:
            err = f"ERR:{type(e).__name__}"

            if attempt == 2:
                return False, err, None, str(e)

            time.sleep((2 ** attempt) * PAUSE)

    return False, "ERR:unknown", None, "unknown"


def get_users_page(
    session: requests.Session,
    offset: int,
    limit: int,
    fields: Optional[List[str]] = None
) -> Tuple[List[Dict], str, Optional[str]]:
    fields_qs = ""

    if fields:
        fields_qs = "&fields=" + ",".join(fields)

    url = f"{BASE_URL}/users?offset={offset}&limit={limit}{fields_qs}"

    ok, code, response, err = request_with_retries(
        session,
        "GET",
        url
    )

    if not ok or response is None:
        return [], code, err

    try:
        data = response.json()
    except Exception as e:
        return [], code, f"json_error:{e}"

    items = data.get("items") or data.get("data") or []

    if not isinstance(items, list):
        return [], code, "items_not_list"

    return items, code, None


def get_users_paginated(session: requests.Session) -> Iterable[Dict]:
    """
    Mantido por compatibilidade com usos antigos.
    """
    offset = 0

    while True:
        items, code, err = get_users_page(session, offset, LIMIT)

        print(
            f"[cleanup] GET /users offset={offset} "
            f"-> code={code} items={len(items)} err={err}"
        )

        if not items:
            break

        for item in items:
            yield item

        got = len(items)
        offset += got

        time.sleep(PAUSE)

        if got < LIMIT:
            break


def delete_user(
    session: requests.Session,
    login: str,
    apply_changes: bool
) -> str:
    """
    DELETE /users/{login}.
    Retorna código HTTP, DRY_RUN ou erro.
    """
    if not login:
        return "NO_LOGIN"

    if not apply_changes:
        return "DRY_RUN"

    url = f"{BASE_URL}/users/{login}"

    ok, code, _, err = request_with_retries(
        session,
        "DELETE",
        url,
        request_timeout=CLEANUP_ACTION_TIMEOUT
    )

    if not ok:
        return code or f"ERR:{err}"

    return code


def inactivate_resource(
    session: requests.Session,
    resource_id: Optional[str],
    apply_changes: bool
) -> str:
    """
    PATCH /resources/{resourceId} com status inactive.
    Retorna código HTTP, NO_RESOURCE, DRY_RUN ou erro.
    """
    if not resource_id:
        return "NO_RESOURCE"

    if not apply_changes:
        return "DRY_RUN"

    url = f"{BASE_URL}/resources/{resource_id}"

    ok, code, _, err = request_with_retries(
        session,
        "PATCH",
        url,
        json={"status": "inactive"},
        request_timeout=CLEANUP_ACTION_TIMEOUT
    )

    if not ok:
        return code or f"ERR:{err}"

    return code


def find_stale_users(
    cutoff_days: int,
    only_active: bool,
    only_logged_once: bool = False,
    progress_callback=None
) -> Tuple[List[Dict], Dict]:
    """
    Retorna usuários candidatos à desativação.

    Regras:
    - only_active=True: considera somente status active.
    - only_logged_once=True: considera somente usuários com lastLoginTime válido.
    - only_logged_once=False: mantém compatibilidade e usuários sem login podem entrar.
    """
    session = get_session()

    user_fields = [
        "login",
        "status",
        "lastLoginTime",
        "userType",
        "mainResourceId",
        "name",
    ]

    first_items, first_code, first_err = get_users_page(
        session,
        0,
        LIMIT,
        fields=user_fields
    )

    if not first_items and first_code not in ("200", "204"):
        return [], {
            "ok": False,
            "first_code": first_code,
            "first_count": 0,
            "total": 0,
            "sample_keys": [],
            "error": first_err,
            "ignored_without_login": 0,
            "ignored_inactive": 0,
            "candidates": 0,
        }

    sample_keys = list(first_items[0].keys())[:8] if first_items else []

    total = 0
    ignored_without_login = 0
    ignored_inactive = 0
    vencidos: List[Dict] = []
    page_error = None

    def maybe_add(user: Dict) -> None:
        nonlocal ignored_without_login, ignored_inactive

        login = (user.get("login") or "").strip()
        status = (user.get("status") or "").strip()

        last_raw = (
            user.get("lastLoginTime")
            or user.get("last_login_time")
            or user.get("last_login")
        )

        user_type = user.get("userType") or user.get("user_type")
        main_resource = user.get("mainResourceId") or user.get("main_resource_id")
        name = user.get("name")

        if not login:
            return

        if only_active and status.lower() != "active":
            ignored_inactive += 1
            return

        parsed_last_login = parse_last_login(last_raw)

        if only_logged_once and parsed_last_login is None:
            ignored_without_login += 1
            return

        if older_than(parsed_last_login, cutoff_days):
            vencidos.append({
                "login": login,
                "name": name,
                "status": status,
                "lastLoginTime": last_raw,
                "userType": user_type,
                "mainResourceId": main_resource,
            })

    for user in first_items:
        total += 1
        maybe_add(user)

    if progress_callback:
        progress_callback({
            "phase": "Processando usuários do OFS",
            "total_scanned": total,
            "candidates": len(vencidos),
            "ignored_without_login": ignored_without_login,
            "ignored_inactive": ignored_inactive,
        })

    offset = len(first_items)

    while offset > 0:
        items, code, err = get_users_page(
            session,
            offset,
            LIMIT,
            fields=user_fields
        )

        if code not in ("200", "204"):
            page_error = err or (
                f"Falha ao buscar página com offset={offset} "
                f"(status {code})"
            )
            break

        if not items:
            break

        for user in items:
            total += 1
            maybe_add(user)

        if progress_callback:
            progress_callback({
                "phase": "Processando usuários do OFS",
                "total_scanned": total,
                "candidates": len(vencidos),
                "ignored_without_login": ignored_without_login,
                "ignored_inactive": ignored_inactive,
            })

        offset += len(items)

        if len(items) < LIMIT:
            break

        time.sleep(PAUSE)

    return vencidos, {
        "ok": page_error is None,
        "first_code": first_code,
        "first_count": len(first_items),
        "total": total,
        "sample_keys": sample_keys,
        "error": page_error,
        "ignored_without_login": ignored_without_login,
        "ignored_inactive": ignored_inactive,
        "candidates": len(vencidos),
    }


def execute_cleanup(
    vencidos: List[Dict],
    apply_changes: bool,
    progress_callback=None
) -> List[Dict]:
    """
    Executa delete de usuário + inativação de recurso.

    Importante:
    - Atualiza progresso antes de cada usuário, para a tela não parecer travada.
    - Não derruba o processamento inteiro se um usuário falhar.
    """
    session = get_session()
    results: List[Dict] = []
    total = len(vencidos)

    for index, user in enumerate(vencidos, start=1):
        login = user.get("login")
        last_raw = user.get("lastLoginTime")
        user_type = user.get("userType")
        main_resource = user.get("mainResourceId")
        name = user.get("name")
        status = user.get("status")

        if progress_callback:
            progress_callback({
                "processed": index - 1,
                "total": total,
                "login": login,
                "phase": f"Processando {login}",
                "delete_user": "-",
                "inactivate_resource": "-",
            })

        try:
            code_delete = delete_user(
                session,
                login,
                apply_changes
            )
        except Exception as e:
            code_delete = f"ERR:{type(e).__name__}:{e}"

        try:
            code_inactivate = inactivate_resource(
                session,
                main_resource,
                apply_changes
            )
        except Exception as e:
            code_inactivate = f"ERR:{type(e).__name__}:{e}"

        result = {
            "login": login,
            "name": name,
            "status": status,
            "userType": user_type,
            "lastLoginTime": last_raw,
            "mainResourceId": main_resource,
            "delete_user": code_delete,
            "inactivate_resource": code_inactivate,
        }

        results.append(result)

        if progress_callback:
            progress_callback({
                "processed": index,
                "total": total,
                "login": login,
                "phase": f"Aplicando limpeza ({index}/{total})",
                "delete_user": code_delete,
                "inactivate_resource": code_inactivate,
            })

        time.sleep(PAUSE)

    return results