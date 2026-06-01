from flask import render_template, request, redirect, url_for, flash, session, send_file, jsonify
from datetime import datetime
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

    @app.route("/desativar_inativos", methods=["GET", "POST"])
    @login_required
    @perm_required("ofs.desativar")
    def desativar_inativos():
        raw_days = (request.values.get("cutoff_days") or "80").strip()
        cutoff_days = int(raw_days) if raw_days.isdigit() else 80

        only_active = request.values.get("only_active") is not None
        only_logged_once = request.values.get("only_logged_once") is not None

        vencidos, meta = find_stale_users(
            cutoff_days=cutoff_days,
            only_active=only_active,
            only_logged_once=only_logged_once
        )

        results = []
        mode = "SIMULACAO"

        if request.method == "POST":
            apply = request.form.get("apply_changes") == "1"

            results = execute_cleanup(vencidos, apply_changes=apply)

            mode = "APLICACAO" if apply else "SIMULACAO"

            flash(
                f"{'Aplicado' if apply else 'Simulado'} para {len(vencidos)} usuários.",
                "success"
            )

            actor = current_actor()

            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="cleanup" if apply else "cleanup_simulation",
                entity_type="ofs_user",
                summary=f"Cleanup OFS: mode={mode}, cutoff_days={cutoff_days}, only_active={only_active}, only_logged_once={only_logged_once}, total={len(vencidos)}",
                meta={
                    "mode": mode,
                    "cutoff_days": cutoff_days,
                    "only_active": only_active,
                    "only_logged_once": only_logged_once,
                    "total": len(vencidos),
                },
            )

        if request.values.get("export") == "1":

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = f"/tmp/users_vencidos_{stamp}.csv"

            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)

                w.writerow([
                    "login",
                    "status",
                    "lastLoginTime",
                    "userType",
                    "mainResourceId",
                ])

                for u in vencidos:
                    w.writerow([
                        u.get("login"),
                        u.get("status"),
                        u.get("lastLoginTime"),
                        u.get("userType"),
                        u.get("mainResourceId"),
                    ])

            return send_file(
                path,
                as_attachment=True,
                download_name=os.path.basename(path),
                mimetype="text/csv"
            )

        return render_template(
            "desativar_inativos.html",
            cutoff_days=cutoff_days,
            only_active=only_active,
            only_logged_once=only_logged_once,
            vencidos=vencidos,
            results=results,
            mode=mode,
            meta=meta,
        )