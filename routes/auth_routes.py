from flask import render_template, request, redirect, url_for, flash, session
from datetime import datetime
import bcrypt

from database.connection import get_connection
from database.audit import audit_log
from core.auth import login_required, perm_required, current_actor

def init_app(app):

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = (request.form.get("username") or "").strip().lower()
            password = (request.form.get("password") or "").strip()

            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM usuarios WHERE username = %s", (username,))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user and bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
                conn = get_connection()
                cur_upd = conn.cursor()
                cur_upd.execute(
                    "UPDATE usuarios SET last_login = %s WHERE id = %s",
                    (datetime.now(), user["id"]),
                )
                conn.commit()
                cur_upd.close()
                conn.close()

                session["usuario_id"] = user["id"]
                session["usuario_logado"] = user["username"]
                session["nome_usuario"] = user["nome"]
                session["tipo_id"] = int(user["tipo_id"]) if user.get("tipo_id") is not None else 3

                from core.auth import _carregar_permissoes_por_perfil
                session["permissoes"] = _carregar_permissoes_por_perfil(session["tipo_id"])

                audit_log(
                    actor_user_id=user["id"],
                    actor_username=user["username"],
                    module="auth",
                    action="login",
                    entity_type="usuario",
                    entity_id=user["id"],
                    entity_ref=user["username"],
                    summary=f"Login realizado com sucesso: {user['username']}",
                    meta={"ip": request.remote_addr, "ua": request.user_agent.string},
                )

                return redirect(url_for("home"))

            audit_log(
                actor_user_id=None,
                actor_username=username,
                module="auth",
                action="login_failed",
                entity_type="usuario",
                entity_ref=username,
                summary=f"Tentativa de login falhou: {username}",
                meta={"ip": request.remote_addr, "ua": request.user_agent.string},
            )

            flash("Usuário ou senha inválidos.", "danger")

        return render_template("login.html")

    @app.route("/logout")
    def logout():
        actor = current_actor()

        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="auth",
            action="logout",
            entity_type="usuario",
            entity_id=actor.get("id"),
            entity_ref=actor.get("username"),
            summary=f"Logout realizado: {actor.get('username')}",
            meta={"ip": request.remote_addr, "ua": request.user_agent.string},
        )

        usuario_id = session.get("usuario_id")

        if usuario_id:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM usuarios_online WHERE usuario_id = %s",
                (usuario_id,),
            )
            conn.commit()
            cur.close()
            conn.close()

        session.clear()

        return redirect(url_for("login"))

    @app.route("/trocar-senha", methods=["GET", "POST"])
    @login_required
    @perm_required("usuarios.trocar_senha")
    def trocar_senha():
        if request.method == "POST":
            senha_atual = (request.form.get("senha_atual") or "").strip()
            nova_senha = (request.form.get("nova_senha") or "").strip()
            confirmar = (request.form.get("confirmar_senha") or "").strip()

            if not senha_atual or not nova_senha or not confirmar:
                flash("Preencha todos os campos.", "danger")
                return redirect(url_for("trocar_senha"))

            if len(nova_senha) < 8:
                flash("A nova senha deve ter pelo menos 8 caracteres.", "danger")
                return redirect(url_for("trocar_senha"))

            if nova_senha != confirmar:
                flash("A confirmação não confere com a nova senha.", "danger")
                return redirect(url_for("trocar_senha"))

            username = session.get("usuario_logado")

            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, password_hash FROM usuarios WHERE username = %s", (username,))
            user = cursor.fetchone()

            if not user:
                cursor.close()
                conn.close()
                flash("Usuário não encontrado.", "danger")
                return redirect(url_for("trocar_senha"))

            if not bcrypt.checkpw(senha_atual.encode(), user["password_hash"].encode()):
                cursor.close()
                conn.close()
                flash("Senha atual incorreta.", "danger")
                return redirect(url_for("trocar_senha"))

            novo_hash = bcrypt.hashpw(nova_senha.encode(), bcrypt.gensalt()).decode()
            cursor.execute("UPDATE usuarios SET password_hash = %s WHERE id = %s", (novo_hash, user["id"]))
            conn.commit()
            cursor.close()
            conn.close()

            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="usuarios",
                action="change_password",
                entity_type="usuario",
                entity_id=user["id"],
                entity_ref=actor.get("username"),
                summary=f"Trocou a própria senha: {actor.get('username')}",
                meta={"ip": request.remote_addr},
            )

            flash("Senha alterada com sucesso!", "success")
            return redirect(url_for("home"))

        return render_template("trocar_senha.html")

    @app.route("/criar-usuario", methods=["GET", "POST"])
    @login_required
    @perm_required("usuarios.criar")
    def criar_usuario():
        if request.method == "POST":
            nome = (request.form.get("nome") or "").strip()

            local = (request.form.get("username_local") or "").strip().lower()
            dominio = "@verointernet.com.br"
            username = local + dominio

            senha = (request.form.get("senha") or "").strip()
            confirmar = (request.form.get("confirmar") or "").strip()
            tipo_id_raw = (request.form.get("tipo_id") or "").strip()

            if not nome or not local or not senha or not confirmar or not tipo_id_raw:
                flash("Preencha todos os campos.", "danger")
                return redirect(url_for("criar_usuario"))

            if not local.replace(".", "").replace("_", "").replace("-", "").isalnum():
                flash("A parte inicial do e-mail contém caracteres inválidos.", "danger")
                return redirect(url_for("criar_usuario"))

            if len(senha) < 8:
                flash("A senha deve ter pelo menos 8 caracteres.", "danger")
                return redirect(url_for("criar_usuario"))

            if senha != confirmar:
                flash("A confirmação não confere com a senha.", "danger")
                return redirect(url_for("criar_usuario"))

            try:
                tipo_id = int(tipo_id_raw)
            except ValueError:
                flash("Perfil inválido.", "danger")
                return redirect(url_for("criar_usuario"))

            conn = get_connection()
            cur = conn.cursor(dictionary=True)

            cur.execute("SELECT id, nome FROM perfis WHERE id = %s", (tipo_id,))
            perfil_row = cur.fetchone()
            if not perfil_row:
                cur.close()
                conn.close()
                flash("Perfil informado não existe.", "danger")
                return redirect(url_for("criar_usuario"))

            cur.execute("SELECT id FROM usuarios WHERE username = %s", (username,))
            if cur.fetchone():
                cur.close()
                conn.close()
                flash("Já existe um usuário com esse e-mail.", "danger")
                return redirect(url_for("criar_usuario"))

            password_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
            cur.execute(
                "INSERT INTO usuarios (nome, username, password_hash, tipo_id) VALUES (%s, %s, %s, %s)",
                (nome, username, password_hash, tipo_id)
            )
            conn.commit()
            cur.close()
            conn.close()

            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="usuarios",
                action="create",
                entity_type="usuario",
                entity_ref=username,
                summary=f"Criou usuário do painel: {username}",
                after={"nome": nome, "username": username, "tipo_id": tipo_id, "perfil": perfil_row.get("nome")},
            )

            flash("Usuário criado com sucesso!", "success")
            return redirect(url_for("criar_usuario"))

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, nome FROM perfis ORDER BY nome")
        perfis = cur.fetchall()
        cur.close()
        conn.close()

        return render_template("criar_usuario.html", perfis=perfis)