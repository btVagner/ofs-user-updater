from flask import render_template, request, redirect, url_for, flash, session, send_file
from datetime import datetime
from io import StringIO
import csv
import os

from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from ofs.cleanup import find_stale_users, execute_cleanup
from core.auth import login_required, perm_required, current_actor


def init_app(app):

    @app.route("/consultar-usuarios")
    @login_required
    @perm_required("ofs.consultar")
    def consultar_usuarios():
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
            })

        ativos = sum(1 for u in usuarios if u["status"] == "active")

        return render_template(
            "consultar_usuarios.html",
            usuarios=usuarios,
            total_ativos=ativos
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