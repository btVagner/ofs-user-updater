from flask import render_template, request, redirect, url_for, flash, session, send_file, jsonify
from datetime import datetime, timedelta
from werkzeug.security import check_password_hash
from io import StringIO
import csv
import os
import json
import uuid
import time
import threading
from pathlib import Path
from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from ofs.cleanup import find_stale_users, execute_cleanup
from core.auth import login_required, perm_required, current_actor

def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _ensure_ofs_users_export_dir(base_dir: str):
    Path(base_dir).mkdir(parents=True, exist_ok=True)


def _ofs_users_job_paths(base_dir: str, job_id: str):
    return {
        "status": os.path.join(base_dir, f"{job_id}.json"),
        "csv": os.path.join(base_dir, f"{job_id}.csv"),
    }


def _write_ofs_users_job_status(base_dir: str, job_id: str, payload: dict):
    _ensure_ofs_users_export_dir(base_dir)

    paths = _ofs_users_job_paths(base_dir, job_id)

    tmp_path = os.path.join(base_dir, f"{job_id}.{uuid.uuid4().hex}.json.tmp")

    data = json.dumps(payload, ensure_ascii=False, indent=2)

    last_error = None

    for _ in range(10):
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, paths["status"])
            return

        except PermissionError as e:
            last_error = e
            time.sleep(0.15)

        except OSError as e:
            last_error = e
            time.sleep(0.15)

        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    raise last_error

def _read_ofs_users_job_status(base_dir: str, job_id: str):
    paths = _ofs_users_job_paths(base_dir, job_id)

    if not os.path.exists(paths["status"]):
        return None

    last_error = None

    for _ in range(5):
        try:
            with open(paths["status"], "r", encoding="utf-8") as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            last_error = e
            time.sleep(0.1)

        except PermissionError as e:
            last_error = e
            time.sleep(0.1)

        except OSError as e:
            last_error = e
            time.sleep(0.1)

    raise last_error


def _run_export_usuarios_ofs_job(base_dir: str, job_id: str, actor: dict):
    paths = _ofs_users_job_paths(base_dir, job_id)

    filename = f"usuarios_ofs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id[:8]}.csv"

    status_payload = {
        "status": "running",
        "phase": "Iniciando exportação",
        "created_at": _now_iso(),
        "finished_at": None,
        "total_users": 0,
        "processed_users": 0,
        "processed_resources": 0,
        "total_resources": 0,
        "progress_percent": 1,
        "filename": filename,
        "error": None,
    }

    try:
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        client = OFSClient()

        # 1. Buscar usuários no OFS
        status_payload["phase"] = "Buscando usuários no OFS"
        status_payload["progress_percent"] = 5
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        usuarios_raw = client.get_usuarios()
        total_users = len(usuarios_raw)

        status_payload["phase"] = f"Usuários carregados: {total_users}"
        status_payload["total_users"] = total_users
        status_payload["processed_users"] = 0
        status_payload["progress_percent"] = 20
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        # 2. Buscar mapa completo de recursos/buckets/hierarquia
        def on_resources_progress(loaded):
            status_payload["phase"] = (
                f"Buscando mapa de recursos e buckets no OFS "
                f"({loaded} recursos carregados)"
            )
            status_payload["processed_resources"] = loaded

            # Progresso aproximado da fase de recursos:
            # começa em 20% e vai até no máximo 70%.
            estimated_percent = 20 + min(50, loaded // 50)
            status_payload["progress_percent"] = min(70, estimated_percent)

            _write_ofs_users_job_status(base_dir, job_id, status_payload)

        status_payload["phase"] = "Buscando mapa de recursos e buckets no OFS"
        status_payload["progress_percent"] = 20
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        hierarchy = client.get_resources_hierarchy_map(
            progress_callback=on_resources_progress
        )

        bucket_map = hierarchy["bucket_by_resource"]
        bucket_name_map = hierarchy["bucket_name_by_resource"]

        bucket_parent_map = hierarchy["bucket_parent_by_resource"]
        bucket_parent_name_map = hierarchy["bucket_parent_name_by_resource"]

        bucket_grandparent_map = hierarchy["bucket_grandparent_by_resource"]
        bucket_grandparent_name_map = hierarchy["bucket_grandparent_name_by_resource"]

        status_payload["phase"] = "Mapa de recursos e buckets carregado"
        status_payload["progress_percent"] = 75
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        # 3. Gerar CSV
        status_payload["phase"] = "Gerando arquivo CSV"
        status_payload["progress_percent"] = 75
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        with open(paths["csv"], "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")

            writer.writerow([
                "Nome",
                "UserType",
                "Bucket ID",
                "Bucket Nome",
                "Recurso Acima do Bucket ID",
                "Recurso Acima do Bucket Nome",
                "Segundo Nível Acima do Bucket ID",
                "Segundo Nível Acima do Bucket Nome",
                "XU_CODE_SAP",
                "Status",
                "Login",
                "Last Login",
                "Main Resource ID",
            ])

            for idx, u in enumerate(usuarios_raw, start=1):
                main_res = u.get("mainResourceId") or u.get("main_resource_id")
                main_res_key = str(main_res) if main_res else ""

                bucket = bucket_map.get(main_res_key, "-") if main_res_key else "-"
                bucket_name = bucket_name_map.get(main_res_key, "-") if main_res_key else "-"

                bucket_parent = bucket_parent_map.get(main_res_key, "-") if main_res_key else "-"
                bucket_parent_name = bucket_parent_name_map.get(main_res_key, "-") if main_res_key else "-"

                bucket_grandparent = bucket_grandparent_map.get(main_res_key, "-") if main_res_key else "-"
                bucket_grandparent_name = bucket_grandparent_name_map.get(main_res_key, "-") if main_res_key else "-"

                writer.writerow([
                    u.get("name", "-"),
                    u.get("userType", "-"),
                    bucket,
                    bucket_name,
                    bucket_parent,
                    bucket_parent_name,
                    bucket_grandparent,
                    bucket_grandparent_name,
                    u.get("XU_CODE_SAP", "-"),
                    u.get("status", "-"),
                    u.get("login", "-"),
                    u.get("lastLoginTime", "-"),
                    main_res or "-",
                ])

                if idx == 1 or idx == total_users or idx % 25 == 0:
                    csv_percent = 75

                    if total_users > 0:
                        csv_percent = 75 + round((idx / total_users) * 25)

                    status_payload["processed_users"] = idx
                    status_payload["phase"] = f"Gerando CSV ({idx}/{total_users})"
                    status_payload["progress_percent"] = min(99, csv_percent)

                    _write_ofs_users_job_status(base_dir, job_id, status_payload)

        # 4. Finalizar
        status_payload["status"] = "success"
        status_payload["phase"] = "Exportação concluída"
        status_payload["finished_at"] = _now_iso()
        status_payload["processed_users"] = total_users
        status_payload["progress_percent"] = 100
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="export_users_csv",
                entity_type="ofs_user",
                summary=f"Exportou usuários OFS para CSV: total={total_users}",
                meta={
                    "job_id": job_id,
                    "filename": filename,
                    "total_users": total_users,
                    "processed_resources": status_payload.get("processed_resources"),
                    "bucket_source": "resources_hierarchy_map",
                    "hierarchy_levels": 3,
                },
            )
        except Exception:
            pass

    except Exception as e:
        status_payload["status"] = "error"
        status_payload["phase"] = "Erro na exportação"
        status_payload["finished_at"] = _now_iso()
        status_payload["error"] = str(e)
        _write_ofs_users_job_status(base_dir, job_id, status_payload)

def _ensure_cleanup_dir(base_dir: str):
    Path(base_dir).mkdir(parents=True, exist_ok=True)


def _cleanup_job_paths(base_dir: str, job_id: str):
    return {
        "status": os.path.join(base_dir, f"{job_id}.json"),
        "candidates": os.path.join(base_dir, f"{job_id}.candidates.json"),
        "results": os.path.join(base_dir, f"{job_id}.results.json"),
        "csv": os.path.join(base_dir, f"{job_id}.csv"),
    }


def _write_cleanup_json(path: str, payload: dict):
    """
    Escrita segura de JSON com retry.

    No Windows/OneDrive, os.replace pode falhar com PermissionError
    quando o arquivo final está sendo lido pelo polling da tela,
    pelo antivírus ou pela sincronização do OneDrive.
    """
    base_dir = os.path.dirname(path)

    if base_dir:
        Path(base_dir).mkdir(parents=True, exist_ok=True)

    data = json.dumps(payload, ensure_ascii=False, indent=2)

    last_error = None

    for attempt in range(20):
        tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, path)
            return

        except PermissionError as e:
            last_error = e
            time.sleep(0.15 + (attempt * 0.03))

        except OSError as e:
            last_error = e
            time.sleep(0.15 + (attempt * 0.03))

        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    raise last_error

def _read_cleanup_json(path: str, default=None):
    """
    Leitura segura de JSON com retry.

    Evita falha quando a tela consulta o status exatamente no momento
    em que o worker está substituindo o arquivo.
    """
    if not os.path.exists(path):
        return default

    last_error = None

    for _ in range(10):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            last_error = e
            time.sleep(0.1)

        except PermissionError as e:
            last_error = e
            time.sleep(0.1)

        except OSError as e:
            last_error = e
            time.sleep(0.1)

    if default is not None:
        return default

    raise last_error

def _write_cleanup_status(base_dir: str, job_id: str, payload: dict):
    _ensure_cleanup_dir(base_dir)
    paths = _cleanup_job_paths(base_dir, job_id)
    _write_cleanup_json(paths["status"], payload)


def _read_cleanup_status(base_dir: str, job_id: str):
    paths = _cleanup_job_paths(base_dir, job_id)
    return _read_cleanup_json(paths["status"])


def _write_cleanup_csv(path: str, rows: list, row_type: str = "candidates"):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")

        if row_type == "results":
            writer.writerow([
                "login",
                "name",
                "status",
                "lastLoginTime",
                "userType",
                "mainResourceId",
                "delete_user",
                "inactivate_resource",
            ])

            for row in rows:
                writer.writerow([
                    row.get("login", ""),
                    row.get("name", ""),
                    row.get("status", ""),
                    row.get("lastLoginTime", ""),
                    row.get("userType", ""),
                    row.get("mainResourceId", ""),
                    row.get("delete_user", ""),
                    row.get("inactivate_resource", ""),
                ])

            return

        writer.writerow([
            "login",
            "name",
            "status",
            "lastLoginTime",
            "userType",
            "mainResourceId",
        ])

        for row in rows:
            writer.writerow([
                row.get("login", ""),
                row.get("name", ""),
                row.get("status", ""),
                row.get("lastLoginTime", ""),
                row.get("userType", ""),
                row.get("mainResourceId", ""),
            ])


def _cleanup_apply_enabled():
    return os.getenv("OFS_CLEANUP_ENABLE_APPLY", "false").lower() in ("1", "true", "yes", "on")


def _cleanup_preview(rows: list, limit: int = 300):
    return rows[:limit]

def _run_cleanup_simulation_job(base_dir: str, job_id: str, actor: dict, config: dict):
    paths = _cleanup_job_paths(base_dir, job_id)

    status_payload = {
        "ok": True,
        "job_id": job_id,
        "job_type": "simulation",
        "status": "running",
        "phase": "Iniciando simulação",
        "created_at": _now_iso(),
        "finished_at": None,
        "progress_percent": 1,
        "cutoff_days": config["cutoff_days"],
        "only_active": config["only_active"],
        "only_logged_once": config["only_logged_once"],
        "total_scanned": 0,
        "candidates": 0,
        "ignored_without_login": 0,
        "ignored_inactive": 0,
        "error": None,
    }

    try:
        _write_cleanup_status(base_dir, job_id, status_payload)

        def on_progress(progress):
            total_scanned = int(progress.get("total_scanned") or 0)
            candidates = int(progress.get("candidates") or 0)

            status_payload["phase"] = progress.get("phase") or "Processando usuários"
            status_payload["total_scanned"] = total_scanned
            status_payload["candidates"] = candidates
            status_payload["ignored_without_login"] = int(progress.get("ignored_without_login") or 0)
            status_payload["ignored_inactive"] = int(progress.get("ignored_inactive") or 0)

            estimated = 5 + min(85, total_scanned // 50)
            status_payload["progress_percent"] = min(90, estimated)

            _write_cleanup_status(base_dir, job_id, status_payload)

        vencidos, meta = find_stale_users(
            cutoff_days=config["cutoff_days"],
            only_active=config["only_active"],
            only_logged_once=config["only_logged_once"],
            progress_callback=on_progress,
        )

        status_payload["phase"] = "Gravando prévia"
        status_payload["progress_percent"] = 92
        _write_cleanup_status(base_dir, job_id, status_payload)

        _write_cleanup_json(paths["candidates"], vencidos)
        _write_cleanup_csv(paths["csv"], vencidos, row_type="candidates")

        status_payload.update({
            "status": "success",
            "phase": "Simulação concluída",
            "finished_at": _now_iso(),
            "progress_percent": 100,
            "total_scanned": meta.get("total", 0),
            "candidates": len(vencidos),
            "ignored_without_login": meta.get("ignored_without_login", 0),
            "ignored_inactive": meta.get("ignored_inactive", 0),
            "meta": meta,
        })

        _write_cleanup_status(base_dir, job_id, status_payload)

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="cleanup_simulation_async",
                entity_type="ofs_user",
                summary=(
                    f"Simulação cleanup OFS: cutoff_days={config['cutoff_days']}, "
                    f"only_active={config['only_active']}, "
                    f"only_logged_once={config['only_logged_once']}, "
                    f"candidates={len(vencidos)}"
                ),
                meta={
                    "job_id": job_id,
                    "config": config,
                    "candidates": len(vencidos),
                    "meta": meta,
                },
            )
        except Exception:
            pass

    except Exception as e:
        status_payload["status"] = "error"
        status_payload["phase"] = "Erro na simulação"
        status_payload["finished_at"] = _now_iso()
        status_payload["error"] = str(e)
        _write_cleanup_status(base_dir, job_id, status_payload)


def _run_cleanup_apply_job(
    base_dir: str,
    apply_job_id: str,
    source_job_id: str,
    actor: dict,
    candidates: list,
):
    apply_paths = _cleanup_job_paths(base_dir, apply_job_id)
    total = len(candidates or [])

    status_payload = {
        "ok": True,
        "job_id": apply_job_id,
        "source_job_id": source_job_id,
        "job_type": "application",
        "status": "running",
        "phase": "Preparando aplicação",
        "created_at": _now_iso(),
        "finished_at": None,
        "progress_percent": 1,
        "processed": 0,
        "total": total,
        "error": None,
    }

    try:
        _cleanup_debug_log(base_dir, apply_job_id, "worker_started", {
            "source_job_id": source_job_id,
            "total_candidates": total,
        })

        _write_cleanup_status(base_dir, apply_job_id, status_payload)

        _cleanup_debug_log(base_dir, apply_job_id, "initial_status_written")

        if not candidates:
            raise RuntimeError("A simulação não possui usuários candidatos.")

        status_payload["phase"] = f"Aplicando limpeza em {total} usuários"
        status_payload["progress_percent"] = 5
        status_payload["processed"] = 0
        status_payload["total"] = total

        _cleanup_debug_log(base_dir, apply_job_id, "before_write_apply_start_status", {
            "phase": status_payload["phase"],
            "total": total,
        })

        _write_cleanup_status(base_dir, apply_job_id, status_payload)

        _cleanup_debug_log(base_dir, apply_job_id, "apply_start_status_written")

        def on_progress(progress):
            processed = int(progress.get("processed") or 0)

            status_payload["processed"] = processed
            status_payload["total"] = total
            status_payload["phase"] = progress.get("phase") or f"Aplicando limpeza ({processed}/{total})"
            status_payload["progress_percent"] = (
                min(99, 5 + round((processed / total) * 94))
                if total
                else 99
            )
            status_payload["current_login"] = progress.get("login")
            status_payload["last_delete_user"] = progress.get("delete_user")
            status_payload["last_inactivate_resource"] = progress.get("inactivate_resource")

            _write_cleanup_status(base_dir, apply_job_id, status_payload)

        _cleanup_debug_log(base_dir, apply_job_id, "before_execute_cleanup")

        results = execute_cleanup(
            candidates,
            apply_changes=True,
            progress_callback=on_progress,
        )

        _cleanup_debug_log(base_dir, apply_job_id, "after_execute_cleanup", {
            "results": len(results),
        })

        _write_cleanup_json(apply_paths["results"], results)
        _write_cleanup_csv(apply_paths["csv"], results, row_type="results")

        status_payload.update({
            "status": "success",
            "phase": "Aplicação concluída",
            "finished_at": _now_iso(),
            "progress_percent": 100,
            "processed": len(results),
            "total": total,
            "error": None,
        })

        _write_cleanup_status(base_dir, apply_job_id, status_payload)

        _cleanup_debug_log(base_dir, apply_job_id, "worker_finished_success")

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="cleanup_async_apply",
                entity_type="ofs_user",
                summary=f"Aplicou cleanup OFS: source_job_id={source_job_id}, total={total}",
                meta={
                    "apply_job_id": apply_job_id,
                    "source_job_id": source_job_id,
                    "total": total,
                    "results_preview": results[:50],
                },
            )
        except Exception:
            pass

    except Exception as e:
        _cleanup_debug_log(base_dir, apply_job_id, "worker_error", {
            "error": str(e),
            "error_type": type(e).__name__,
        })

        status_payload["status"] = "error"
        status_payload["phase"] = "Erro na aplicação"
        status_payload["finished_at"] = _now_iso()
        status_payload["progress_percent"] = 0
        status_payload["error"] = str(e)

        try:
            _write_cleanup_status(base_dir, apply_job_id, status_payload)
        except Exception as write_error:
            _cleanup_debug_log(base_dir, apply_job_id, "worker_error_status_write_failed", {
                "error": str(write_error),
                "error_type": type(write_error).__name__,
            })
def _cleanup_unlock_dir(base_dir: str):
    path = os.path.join(base_dir, "unlock_tokens")
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def _cleanup_unlock_path(base_dir: str, token: str):
    return os.path.join(_cleanup_unlock_dir(base_dir), f"{token}.json")


def _cleanup_password_hash_from_user_row(row: dict):
    return row.get("password_hash")

def _verify_password_value(stored_password: str, password: str) -> bool:
    """
    Valida senha do painel.

    Suporta:
    - bcrypt: $2a$ / $2b$ / $2y$
    - Werkzeug: pbkdf2/scrypt
    - fallback legado para texto puro, apenas por compatibilidade
    """
    if not stored_password or not password:
        return False

    stored_password = str(stored_password).strip()

    # bcrypt
    if stored_password.startswith(("$2a$", "$2b$", "$2y$")):
        try:
            import bcrypt

            return bcrypt.checkpw(
                password.encode("utf-8"),
                stored_password.encode("utf-8")
            )
        except Exception as e:
            print(f"[cleanup-unlock] bcrypt validation error: {e}")
            return False

    # Werkzeug generate_password_hash
    try:
        if check_password_hash(stored_password, password):
            return True
    except Exception:
        pass

    # Fallback antigo, caso algum usuário tenha senha em texto puro
    if stored_password == password:
        return True

    return False
def _verify_current_user_password(password: str) -> bool:
    if not password:
        return False

    actor = current_actor() or {}

    actor_id = actor.get("id")
    actor_username = (
        actor.get("username")
        or actor.get("login")
        or session.get("username")
        or session.get("usuario")
        or session.get("usuario_logado")
        or session.get("nome_usuario")
    )

    conn = get_connection()
    cursor = None

    try:
        cursor = conn.cursor(dictionary=True)

        row = None

        if actor_id:
            cursor.execute(
                """
                SELECT id, username, password_hash
                FROM usuarios
                WHERE id = %s
                LIMIT 1
                """,
                (actor_id,)
            )
            row = cursor.fetchone()

        if not row and actor_username:
            cursor.execute(
                """
                SELECT id, username, password_hash
                FROM usuarios
                WHERE username = %s
                LIMIT 1
                """,
                (actor_username,)
            )
            row = cursor.fetchone()

        if not row:
            print("[cleanup-unlock] usuário não encontrado para validação de senha")
            return False

        stored_password = _cleanup_password_hash_from_user_row(row)

        if not stored_password:
            print("[cleanup-unlock] password_hash vazio")
            return False

        return _verify_password_value(stored_password, password)

    finally:
        try:
            if cursor:
                cursor.close()
            conn.close()
        except Exception:
            pass
def _create_cleanup_unlock_token(base_dir: str, actor: dict, page_session_id: str):
    token = uuid.uuid4().hex
    expires_at = datetime.now() + timedelta(minutes=30)

    payload = {
        "token": token,
        "actor_id": actor.get("id"),
        "actor_username": actor.get("username"),
        "page_session_id": page_session_id,
        "created_at": _now_iso(),
        "expires_at": expires_at.isoformat(timespec="seconds"),
    }

    _write_cleanup_json(_cleanup_unlock_path(base_dir, token), payload)

    return token, payload


def _validate_cleanup_unlock_token(base_dir: str, token: str, page_session_id: str):
    if not token or not page_session_id:
        return False, "Desbloqueio obrigatório para aplicar."

    path = _cleanup_unlock_path(base_dir, token)
    payload = _read_cleanup_json(path)

    if not payload:
        return False, "Desbloqueio não encontrado ou expirado."

    actor = current_actor()

    if str(payload.get("actor_id")) != str(actor.get("id")):
        return False, "Desbloqueio pertence a outro usuário."

    if payload.get("page_session_id") != page_session_id:
        return False, "Desbloqueio não pertence a esta sessão da tela."

    try:
        expires_at = datetime.fromisoformat(payload.get("expires_at"))
    except Exception:
        return False, "Desbloqueio inválido."

    if datetime.now() > expires_at:
        try:
            os.remove(path)
        except Exception:
            pass

        return False, "Desbloqueio expirado. Informe a senha novamente."

    return True, None


def _revoke_cleanup_unlock_token(base_dir: str, token: str):
    if not token:
        return

    path = _cleanup_unlock_path(base_dir, token)

    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass

def _cleanup_debug_log(base_dir: str, job_id: str, message: str, extra: dict = None):
    try:
        _ensure_cleanup_dir(base_dir)

        log_path = os.path.join(base_dir, f"{job_id}.debug.log")

        payload = {
            "at": _now_iso(),
            "message": message,
            "extra": extra or {},
        }

        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            f.flush()

    except Exception as e:
        print(f"[cleanup-debug] falha ao gravar log: {e}")
def init_app(app):

    @app.route("/consultar-usuarios")
    @login_required
    @perm_required("ofs.consultar")
    def consultar_usuarios():
        return render_template("consultar_usuarios.html")
    @app.route("/consultar-usuarios/exportar/iniciar", methods=["POST"])
    @login_required
    @perm_required("ofs.consultar")
    def iniciar_exportacao_usuarios_ofs():
        job_id = uuid.uuid4().hex
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users")
        filename = f"usuarios_ofs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id[:8]}.csv"

        status_payload = {
            "status": "queued",
            "phase": "Exportação na fila",
            "created_at": _now_iso(),
            "finished_at": None,
            "total_users": 0,
            "processed_users": 0,
            "filename": filename,
            "error": None,
        }

        _write_ofs_users_job_status(base_dir, job_id, status_payload)

        actor = current_actor()

        thread = threading.Thread(
            target=_run_export_usuarios_ofs_job,
            args=(base_dir, job_id, actor),
            daemon=True,
        )
        thread.start()

        return jsonify({
            "ok": True,
            "job_id": job_id,
            "status_url": url_for("status_exportacao_usuarios_ofs", job_id=job_id),
            "download_url": url_for("download_exportacao_usuarios_ofs", job_id=job_id),
        })


    @app.route("/consultar-usuarios/exportar/status/<job_id>")
    @login_required
    @perm_required("ofs.consultar")
    def status_exportacao_usuarios_ofs(job_id):
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users")
        status_payload = _read_ofs_users_job_status(base_dir, job_id)

        if not status_payload:
            return jsonify({
                "ok": False,
                "error": "Job não encontrado."
            }), 404

        return jsonify({
            "ok": True,
            **status_payload,
        })


    @app.route("/consultar-usuarios/exportar/download/<job_id>")
    @login_required
    @perm_required("ofs.consultar")
    def download_exportacao_usuarios_ofs(job_id):
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users")
        status_payload = _read_ofs_users_job_status(base_dir, job_id)

        if not status_payload:
            flash("Exportação não encontrada.", "danger")
            return redirect(url_for("consultar_usuarios"))

        if status_payload.get("status") != "success":
            flash("A exportação ainda não foi concluída.", "warning")
            return redirect(url_for("consultar_usuarios"))

        paths = _ofs_users_job_paths(base_dir, job_id)

        if not os.path.exists(paths["csv"]):
            flash("Arquivo CSV não encontrado.", "danger")
            return redirect(url_for("consultar_usuarios"))

        return send_file(
            paths["csv"],
            as_attachment=True,
            download_name=status_payload.get("filename") or f"usuarios_ofs_{job_id[:8]}.csv",
            mimetype="text/csv",
        )
    @app.route("/consultar-usuarios/exportar")
    @login_required
    @perm_required("ofs.consultar")
    def exportar_usuarios_ofs():
        client = OFSClient()
        usuarios_raw = client.get_usuarios()

        bucket_cache = {}
        usuarios = []

        for u in usuarios_raw:
            main_res = u.get("mainResourceId") or u.get("main_resource_id")

            if main_res:
                if main_res in bucket_cache:
                    bucket = bucket_cache[main_res]
                else:
                    try:
                        bucket = client.get_bucket_by_resource_id(main_res)
                    except Exception:
                        bucket = "-"
                    bucket_cache[main_res] = bucket
            else:
                bucket = "-"

            usuarios.append({
                "name": u.get("name", "-"),
                "userType": u.get("userType", "-"),
                "bucket": bucket,
                "code_sap": u.get("XU_CODE_SAP", "-"),
                "status": u.get("status", "-"),
                "login": u.get("login", "-"),
                "lastLoginTime": u.get("lastLoginTime", "-"),
                "mainResourceId": main_res or "-",
            })

        output = StringIO()
        writer = csv.writer(output, delimiter=";")

        writer.writerow([
            "Nome",
            "UserType",
            "Bucket ID",
            "Bucket Nome",
            "Recurso Acima do Bucket ID",
            "Recurso Acima do Bucket Nome",
            "XU_CODE_SAP",
            "Status",
            "Login",
            "Last Login",
            "Main Resource ID",
        ])

        for u in usuarios:
            writer.writerow([
                u["name"],
                u["userType"],
                u["bucket"],
                u["code_sap"],
                u["status"],
                u["login"],
                u["lastLoginTime"],
                u["mainResourceId"],
            ])

        output.seek(0)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"usuarios_ofs_{stamp}.csv"

        return app.response_class(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    @app.route("/atualizar", methods=["GET", "POST"])
    @login_required
    @perm_required("ofs.atualizar_tipo")
    def atualizar_user_type():
        if request.method == "POST":
            resource_id = request.form.get("resource_id")
            new_user_type = request.form.get("user_type")

            client = OFSClient()
            try:
                login = client.get_login_by_resource_id(resource_id)
                status, _ = client.update_user_type(login, new_user_type)
                flash(f"✅ Login {login} atualizado com sucesso! (Status: {status})", "success")
            except Exception as e:
                flash(f"❌ Falha: {e}", "danger")

            return redirect(url_for("atualizar_user_type"))

        return render_template("atualizar_user_type.html")
    
    @app.route("/atualizar-um", methods=["GET", "POST"])
    @login_required
    @perm_required("ofs.atualizar_tipo")
    def atualizar_um():
        tipos_user = session.get("tipos_user", [])

        if request.method == "POST":
            resource_id = request.form.get("resource_id")
            user_type_codigo = request.form.get("user_type")

            session["ultimo_user_type"] = user_type_codigo

            username = os.getenv("OFS_USERNAME")
            password = os.getenv("OFS_PASSWORD")
            client = OFSClient(username, password)

            try:
                login = client.get_login_by_resource_id(resource_id)
                status, _ = client.update_user_type(login, user_type_codigo)
                flash(f"✅ Login {login} atualizado com sucesso! (Status: {status})", "success")

                actor = current_actor()
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="ofs",
                    action="update_user_type",
                    entity_type="ofs_user",
                    entity_ref=str(resource_id),
                    summary=f"Atualizou userType no OFS (um): resourceId={resource_id} login={login} userType={user_type_codigo}",
                    meta={"resource_id": resource_id, "login": login, "userType": user_type_codigo, "status": status},
                )

            except Exception as e:
                flash(f"❌ Erro ao atualizar o userType: {e}", "danger")

            return redirect(url_for("atualizar_um"))

        selected = session.pop("ultimo_user_type", "")
        return render_template("atualizar_um.html", tipos=tipos_user, selected=selected)
    @app.route("/atualizar-varios", methods=["GET", "POST"])
    @login_required
    @perm_required("ofs.atualizar_tipo")
    def atualizar_varios():
        tipos_user = session.get("tipos_user", [])

        if request.method == "POST":
            modo = request.form.get("modo")  # "resourceId" ou "email"
            valores_raw = request.form.get("identificadores", "")
            user_type = request.form.get("user_type")

            valores = [v.strip() for v in valores_raw.split(",") if v.strip()]
            logs = []

            username = os.getenv("OFS_USERNAME")
            password = os.getenv("OFS_PASSWORD")
            client = OFSClient(username, password)

            ok = 0
            fail = 0

            for item in valores:
                try:
                    if modo == "email":
                        login = item
                    else:
                        login = client.get_login_by_resource_id(item)

                    status, _ = client.update_user_type(login, user_type)
                    logs.append(f"✅ {item} → {login} atualizado com sucesso (Status: {status})")
                    ok += 1
                except Exception as e:
                    logs.append(f"❌ {item} → Erro: {e}")
                    fail += 1

            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="bulk_update_user_type",
                entity_type="ofs_user",
                summary=f"Atualizou userType em lote: modo={modo}, userType={user_type}, total={len(valores)}, ok={ok}, fail={fail}",
                meta={
                    "modo": modo,
                    "userType": user_type,
                    "total": len(valores),
                    "ok": ok,
                    "fail": fail,
                    "itens": valores[:200],
                },
            )

            session["log_varios"] = logs
            return redirect(url_for("log_varios"))

        return render_template("atualizar_varios.html", tipos=tipos_user)

    @app.route("/log-varios")
    @login_required
    @perm_required("ofs.atualizar_tipo")
    def log_varios():
        logs = session.pop("log_varios", [])
        return render_template("log_varios.html", logs=logs)

    @app.route("/criar-tecnicos", methods=["GET", "POST"])
    @login_required
    @perm_required("ofs.criar_tecnicos")
    def criar_tecnicos():
        logs = []

        if request.method == "POST":
            if "csv_file" not in request.files or request.files["csv_file"].filename == "":
                flash("Envie um arquivo CSV válido.", "danger")
                return render_template("criar_tecnicos.html", logs=logs)

            file = request.files["csv_file"]

            try:
                data = file.read().decode("utf-8-sig")
            except Exception:
                flash("Falha ao ler o CSV. Verifique se está em UTF-8.", "danger")
                return render_template("criar_tecnicos.html", logs=logs)

            reader = csv.DictReader(StringIO(data))

            expected = [
                "idSAP",
                "depositoTecnico",
                "tipoDeRecurso",
                "nomeCompleto",
                "areaDoTecnico",
                "tipoDeUsuario",
                "email",
                "Senha",
            ]

            missing = [h for h in expected if h not in reader.fieldnames]
            if missing:
                flash(f"Cabeçalhos ausentes no CSV: {', '.join(missing)}", "danger")
                return render_template("criar_tecnicos.html", logs=logs)

            client = OFSClient()

            linha = 1

            for row in reader:
                linha += 1

                id_sap = (row.get("idSAP") or "").strip()
                deposito_tecnico = (row.get("depositoTecnico") or "").strip()
                nome_completo = (row.get("nomeCompleto") or "").strip()
                area_tecnico = (row.get("areaDoTecnico") or "").strip()
                tipo_usuario = (row.get("tipoDeUsuario") or "").strip()
                email = (row.get("email") or "").strip()
                senha = (row.get("Senha") or "").strip()

                msg_parts = []
                rec_status = "-"
                usr_status = "-"
                dep_status = "-"

                if not id_sap or not nome_completo or not area_tecnico or not tipo_usuario or not email or not senha:
                    logs.append({
                        "linha": linha,
                        "idSAP": id_sap,
                        "email": email,
                        "recurso_status": rec_status,
                        "usuario_status": usr_status,
                        "deposito_status": dep_status,
                        "msg": "Dados obrigatórios ausentes na linha."
                    })
                    continue

                try:
                    r1 = client.create_resource(
                        id_sap=id_sap,
                        parent_resource_id=area_tecnico,
                        name=nome_completo,
                        email=email
                    )

                    rec_status = f"{r1.status_code}"

                    if r1.status_code in (200, 201):
                        msg_parts.append("Recurso criado/atualizado com sucesso.")
                    elif r1.status_code == 409:
                        msg_parts.append("Recurso já existia (409).")
                    else:
                        msg_parts.append(f"Falha ao criar recurso: {r1.status_code} {r1.text}")

                except Exception as e:
                    msg_parts.append(f"Exceção na criação do recurso: {e}")

                if rec_status in ("200", "201", "409"):
                    try:
                        r2 = client.create_user(
                            email=email,
                            name=nome_completo,
                            id_sap=id_sap,
                            user_type=tipo_usuario,
                            password=senha
                        )

                        usr_status = f"{r2.status_code}"

                        if r2.status_code in (200, 201):
                            msg_parts.append("Usuário criado/atualizado com sucesso.")
                        elif r2.status_code == 409:
                            msg_parts.append("Usuário já existia (409).")
                        else:
                            msg_parts.append(f"Falha ao criar usuário: {r2.status_code} {r2.text}")

                    except Exception as e:
                        msg_parts.append(f"Exceção na criação do usuário: {e}")

                else:
                    msg_parts.append("Usuário não criado pois o recurso não foi criado.")

                if rec_status in ("200", "201", "409") and deposito_tecnico:
                    try:
                        r3 = client.update_resource_deposito(
                            id_sap=id_sap,
                            deposito_tecnico=deposito_tecnico
                        )

                        dep_status = f"{r3.status_code}"

                        if r3.status_code in (200, 204):
                            msg_parts.append("Depósito atualizado com sucesso.")
                        else:
                            msg_parts.append(f"Falha ao atualizar depósito: {r3.status_code} {r3.text}")

                    except Exception as e:
                        msg_parts.append(f"Exceção no update do depósito: {e}")

                logs.append({
                    "linha": linha,
                    "idSAP": id_sap,
                    "email": email,
                    "recurso_status": rec_status,
                    "usuario_status": usr_status,
                    "deposito_status": dep_status,
                    "msg": "\n".join(msg_parts)
                })

            flash(f"Processamento concluído. Linhas processadas: {len(logs)}", "success")

            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="create_tecnicos_csv",
                entity_type="ofs_user",
                summary=f"Criou técnicos via CSV: linhas={len(logs)}",
                meta={"linhas": len(logs)},
            )

        return render_template("criar_tecnicos.html", logs=logs)

    @app.route("/desativar_inativos", methods=["GET"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos():
        return render_template(
            "desativar_inativos.html",
            cutoff_days=80,
            only_active=True,
            only_logged_once=True,
            apply_unlocked=False,
        )
    @app.route("/desativar_inativos/desbloquear", methods=["POST"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_desbloquear():
        password = request.form.get("password") or ""
        page_session_id = request.form.get("page_session_id") or ""

        if not page_session_id:
            return jsonify({
                "ok": False,
                "error": "Sessão da tela inválida. Recarregue a página."
            }), 400

        if not _verify_current_user_password(password):
            actor = current_actor()

            try:
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="ofs",
                    action="cleanup_unlock_failed",
                    entity_type="ofs_user",
                    summary="Tentativa inválida de desbloqueio da tela de cleanup OFS",
                    meta={"page_session_id": page_session_id},
                )
            except Exception:
                pass

            return jsonify({
                "ok": False,
                "error": "Senha inválida."
            }), 403

        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")
        actor = current_actor()

        token, payload = _create_cleanup_unlock_token(
            base_dir=base_dir,
            actor=actor,
            page_session_id=page_session_id,
        )

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="cleanup_unlock_success",
                entity_type="ofs_user",
                summary="Desbloqueou aplicação da tela de cleanup OFS",
                meta={
                    "page_session_id": page_session_id,
                    "expires_at": payload.get("expires_at"),
                },
            )
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "unlock_token": token,
            "expires_at": payload.get("expires_at"),
        })

    @app.route("/desativar_inativos/bloquear", methods=["POST"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_bloquear():
        token = request.form.get("unlock_token") or ""

        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")
        _revoke_cleanup_unlock_token(base_dir, token)

        return jsonify({"ok": True})
    @app.route("/desativar_inativos/simular", methods=["POST"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_simular():
        raw_days = (request.form.get("cutoff_days") or "80").strip()
        cutoff_days = int(raw_days) if raw_days.isdigit() else 80

        if cutoff_days < 1:
            return jsonify({"ok": False, "error": "Informe um cutoff maior que zero."}), 400

        only_active = request.form.get("only_active") is not None
        only_logged_once = request.form.get("only_logged_once") is not None

        job_id = uuid.uuid4().hex
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")

        config = {
            "cutoff_days": cutoff_days,
            "only_active": only_active,
            "only_logged_once": only_logged_once,
        }

        actor = current_actor()

        initial_status = {
            "ok": True,
            "job_id": job_id,
            "job_type": "simulation",
            "status": "queued",
            "phase": "Simulação na fila",
            "created_at": _now_iso(),
            "finished_at": None,
            "progress_percent": 0,
            "cutoff_days": cutoff_days,
            "only_active": only_active,
            "only_logged_once": only_logged_once,
            "total_scanned": 0,
            "candidates": 0,
            "ignored_without_login": 0,
            "ignored_inactive": 0,
            "error": None,
        }

        _write_cleanup_status(base_dir, job_id, initial_status)

        thread = threading.Thread(
            target=_run_cleanup_simulation_job,
            args=(base_dir, job_id, actor, config),
            daemon=True,
        )
        thread.start()

        return jsonify({
            "ok": True,
            "job_id": job_id,
            "status_url": url_for("desativar_inativos_status", job_id=job_id),
            "download_url": url_for("desativar_inativos_download", job_id=job_id),
        })


    @app.route("/desativar_inativos/status/<job_id>")
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_status(job_id):
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")
        paths = _cleanup_job_paths(base_dir, job_id)

        status_payload = _read_cleanup_status(base_dir, job_id)

        if not status_payload:
            return jsonify({"ok": False, "error": "Job não encontrado."}), 404

        payload = {
            "ok": True,
            **status_payload,
        }

        if status_payload.get("status") == "success":
            if status_payload.get("job_type") == "application":
                rows = _read_cleanup_json(paths["results"], default=[])
                payload["results_preview"] = _cleanup_preview(rows)
                payload["results_total"] = len(rows)
            else:
                rows = _read_cleanup_json(paths["candidates"], default=[])
                payload["candidates_preview"] = _cleanup_preview(rows)
                payload["candidates_total"] = len(rows)

        return jsonify(payload)


    @app.route("/desativar_inativos/download/<job_id>")
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_download(job_id):
        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")
        paths = _cleanup_job_paths(base_dir, job_id)

        status_payload = _read_cleanup_status(base_dir, job_id)

        if not status_payload:
            flash("Job não encontrado.", "danger")
            return redirect(url_for("desativar_inativos"))

        if status_payload.get("status") != "success":
            flash("O processamento ainda não foi concluído.", "warning")
            return redirect(url_for("desativar_inativos"))

        if not os.path.exists(paths["csv"]):
            flash("Arquivo CSV não encontrado.", "danger")
            return redirect(url_for("desativar_inativos"))

        filename_prefix = "resultado_cleanup_ofs" if status_payload.get("job_type") == "application" else "previa_cleanup_ofs"
        filename = f"{filename_prefix}_{job_id[:8]}.csv"

        return send_file(
            paths["csv"],
            as_attachment=True,
            download_name=filename,
            mimetype="text/csv",
        )


    @app.route("/desativar_inativos/aplicar/<job_id>", methods=["POST"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos_aplicar(job_id):
        confirmation = (request.form.get("confirmation") or "").strip().upper()

        if confirmation != "APLICAR":
            return jsonify({
                "ok": False,
                "error": "Confirmação inválida. Digite APLICAR para confirmar."
            }), 400

        unlock_token = request.form.get("unlock_token") or ""
        page_session_id = request.form.get("page_session_id") or ""

        base_dir = os.path.join(app.instance_path, "reports", "ofs_users_cleanup")

        token_ok, token_error = _validate_cleanup_unlock_token(
            base_dir=base_dir,
            token=unlock_token,
            page_session_id=page_session_id,
        )

        if not token_ok:
            return jsonify({
                "ok": False,
                "error": token_error or "Desbloqueio inválido."
            }), 403

        source_paths = _cleanup_job_paths(base_dir, job_id)
        source_status = _read_cleanup_json(source_paths["status"])

        if not source_status:
            return jsonify({
                "ok": False,
                "error": "Job de simulação não encontrado."
            }), 404

        if source_status.get("status") != "success" or source_status.get("job_type") != "simulation":
            return jsonify({
                "ok": False,
                "error": "Apenas simulações concluídas podem ser aplicadas."
            }), 400

        candidates = _read_cleanup_json(source_paths["candidates"], default=[])

        if not candidates:
            return jsonify({
                "ok": False,
                "error": "A simulação não possui usuários candidatos."
            }), 400

        apply_job_id = uuid.uuid4().hex
        actor = current_actor()

        initial_status = {
            "ok": True,
            "job_id": apply_job_id,
            "source_job_id": job_id,
            "job_type": "application",
            "status": "queued",
            "phase": "Aplicação na fila",
            "created_at": _now_iso(),
            "finished_at": None,
            "progress_percent": 0,
            "processed": 0,
            "total": len(candidates),
            "error": None,
        }

        _write_cleanup_status(base_dir, apply_job_id, initial_status)

        thread = threading.Thread(
            target=_run_cleanup_apply_job,
            args=(base_dir, apply_job_id, job_id, actor, candidates),
            daemon=True,
        )
        thread.start()

        return jsonify({
            "ok": True,
            "job_id": apply_job_id,
            "status_url": url_for("desativar_inativos_status", job_id=apply_job_id),
            "download_url": url_for("desativar_inativos_download", job_id=apply_job_id),
        })