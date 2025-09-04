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


BASE_URL  = os.getenv("OFS_BASE_URL", "https://verointernet.fs.ocs.oraclecloud.com/rest/ofscCore/v1").rstrip("/")
USERNAME  = os.getenv("OFS_USERNAME", "XXXXX")
PASSWORD  = os.getenv("OFS_PASSWORD", "XXXXX")

LIMIT     = int(os.getenv("OFS_PAGE_LIMIT", "100"))
TIMEOUT   = int(os.getenv("OFS_TIMEOUT", "30"))
PAUSE     = float(os.getenv("OFS_PAUSE", "0.2"))
VERIFY_SSL = os.getenv("OFS_VERIFY_SSL", "true").lower() not in ("0", "false", "no")

def parse_last_login(s: Optional[str]) -> Optional[datetime]:
    """Aceita ISO (com T/Z) e 'YYYY-MM-DD HH:MM:SS'. Retorna None se vazio/indecifrável."""
    if not s:
        return None
    s = str(s).strip()
    if s.lower() in ("none", "null", "0"):
        return None
    # tolerância a ISO:
    s2 = s.replace("T", " ").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s2)
    except ValueError:
        pass
    # formato simples:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def older_than(dt: Optional[datetime], days: int) -> bool:
    """Sem lastLoginTime -> True. Com data -> True se anterior ao cutoff."""
    if dt is None:
        return True
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    return dt <= (now - timedelta(days=days))

def get_session() -> requests.Session:
    s = requests.Session()
    # Authorization: Basic
    tok = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {tok}",
        "User-Agent": "ofs-cleanup/flask",
    })
    s.verify = VERIFY_SSL
    s.timeout = TIMEOUT
    return s

def request_with_retries(session: requests.Session, method: str, url: str, **kwargs) -> Tuple[bool, str, Optional[requests.Response], Optional[str]]:
    """3 tentativas em 429/5xx; retorna (ok, code_ou_ERR, response, err_text)."""
    for attempt in range(3):
        try:
            resp = session.request(method, url, timeout=TIMEOUT, **kwargs)
            code = str(resp.status_code)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep((2 ** attempt) * PAUSE)
                continue
            return True, code, resp, None
        except requests.RequestException as e:
            err = f"ERR:{type(e).__name__}"
            if attempt == 2:
                return False, err, None, str(e)
            time.sleep((2 ** attempt) * PAUSE)
    return False, "ERR:unknown", None, "unknown"

def get_users_page(session: requests.Session, offset: int, limit: int) -> Tuple[List[Dict], str, Optional[str]]:
    url = f"{BASE_URL}/users?offset={offset}&limit={limit}"
    ok, code, resp, err = request_with_retries(session, "GET", url)
    if not ok or resp is None:
        return [], code, err
    try:
        data = resp.json()
    except Exception as e:
        return [], f"{code}", f"json_error:{e}"
    items = data.get("items") or data.get("data") or []
    if not isinstance(items, list):
        return [], f"{code}", "items_not_list"
    return items, code, None

def get_users_paginated(session: requests.Session) -> Iterable[Dict]:
    """Itera por todos usuários via offset/limit."""
    offset = 0
    while True:
        items, code, err = get_users_page(session, offset, LIMIT)
        # print de diagnóstico no console
        print(f"[cleanup] GET /users offset={offset} -> code={code} items={len(items)} err={err}")
        if not items:
            break
        for it in items:
            yield it
        got = len(items)
        offset += got
        time.sleep(PAUSE)
        if got < LIMIT:
            break

def delete_user(session: requests.Session, login: str, apply_changes: bool) -> str:
    """DELETE /users/{login}. Retorna código HTTP ou 'DRY_RUN'."""
    url = f"{BASE_URL}/users/{login}"
    if not apply_changes:
        return "DRY_RUN"
    ok, code, _, _ = request_with_retries(session, "DELETE", url)
    return code

def inactivate_resource(session: requests.Session, resource_id: Optional[str], apply_changes: bool) -> str:
    """PUT /resources/{id} status=inactive. Retorna código HTTP ou 'NO_RESOURCE'/'DRY_RUN'."""
    if not resource_id:
        return "NO_RESOURCE"
    url = f"{BASE_URL}/resources/{resource_id}"
    if not apply_changes:
        return "DRY_RUN"
    ok, code, _, _ = request_with_retries(session, "PUT", url, json={"status": "inactive"})
    return code

def find_stale_users(cutoff_days: int, only_active: bool) -> Tuple[List[Dict], Dict]:
    """
    Retorna (vencidos, meta):
      meta = {
        "ok": bool, "first_code": str, "first_count": int,
        "total": int, "sample_keys": List[str], "error": Optional[str]
      }
    """
    session = get_session()
    # primeira página p/ diagnóstico
    first_items, first_code, first_err = get_users_page(session, 0, LIMIT)
    if not first_items and first_code not in ("200", "204"):
        return [], {"ok": False, "first_code": first_code, "first_count": 0, "total": 0, "sample_keys": [], "error": first_err}

    sample_keys = list(first_items[0].keys())[:8] if first_items else []
    total = 0
    vencidos: List[Dict] = []

    # processa primeira página
    def maybe_add(u: Dict):
        nonlocal vencidos
        login   = u.get("login")
        status  = u.get("status")
        lastraw = u.get("lastLoginTime") or u.get("last_login_time") or u.get("last_login")  # tolerância a nomes
        utype   = u.get("userType") or u.get("user_type")
        mainres = u.get("mainResourceId") or u.get("main_resource_id")
        if not login:
            return
        if only_active and status != "active":
            return
        if older_than(parse_last_login(lastraw), cutoff_days):
            vencidos.append({
                "login": login,
                "status": status,
                "lastLoginTime": lastraw,
                "userType": utype,
                "mainResourceId": mainres,
            })

    for u in first_items:
        total += 1
        maybe_add(u)

    # demais páginas
    offset = len(first_items)
    while True:
        if offset == 0:
            break  # já não tinha mais itens
        items, code, err = get_users_page(session, offset, LIMIT)
        if not items:
            break
        for u in items:
            total += 1
            maybe_add(u)
        offset += len(items)
        time.sleep(PAUSE)
        if len(items) < LIMIT:
            break

    return vencidos, {
        "ok": True,
        "first_code": first_code,
        "first_count": len(first_items),
        "total": total,
        "sample_keys": sample_keys,
        "error": None
    }

def execute_cleanup(vencidos: List[Dict], apply_changes: bool) -> List[Dict]:
    """Executa delete + inativação e retorna log por usuário."""
    session = get_session()
    results: List[Dict] = []
    for u in vencidos:
        login   = u["login"]
        lastraw = u.get("lastLoginTime")
        utype   = u.get("userType")
        mainres = u.get("mainResourceId")

        code_del   = delete_user(session, login, apply_changes)
        code_inact = inactivate_resource(session, mainres, apply_changes)

        results.append({
            "login": login,
            "userType": utype,
            "lastLoginTime": lastraw,
            "mainResourceId": mainres,
            "delete_user": code_del,
            "inactivate_resource": code_inact,
        })
        time.sleep(PAUSE)
    return results
