from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session, jsonify
import requests
from datetime import datetime, timedelta
import csv
from dotenv import load_dotenv
from database.connection import get_connection
import bcrypt
import os
from functools import wraps
from io import BytesIO, StringIO
import json
import time
from database.audit import audit_log
from werkzeug.middleware.proxy_fix import ProxyFix

load_dotenv()

from ofs.cleanup import find_stale_users, execute_cleanup
from ofs.client import OFSClient

app = Flask(__name__)
app.config["APPLICATION_ROOT"]="/ofs"
app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

@app.before_request
def atualizar_online():
    # Só registra se o usuário estiver logado
    if session.get("usuario_logado"):
        try:
            registrar_atividade_usuario()
        except Exception as e:
            # Em produção você pode logar isso
            print(f"[WARN] Falha ao registrar atividade do usuário: {e}")
@app.context_processor
def inject_online_count():
    """Disponibiliza 'usuarios_online_count' em todos os templates."""
    count = 0
    try:
        conn = get_connection()
        cur = conn.cursor()

        limite = datetime.now() - timedelta(minutes=5)  # janela de 5 minutos
        cur.execute(
            "SELECT COUNT(*) FROM usuarios_online WHERE last_seen >= %s",
            (limite,),
        )
        row = cur.fetchone()
        if row:
            count = row[0]

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WARN] Falha ao obter usuarios_online_count: {e}")

    return dict(usuarios_online_count=count)
def current_actor():
    return {
        "id": session.get("usuario_id"),
        "username": session.get("usuario_logado"),
        "nome": session.get("nome_usuario"),
        "tipo_id": session.get("tipo_id"),
    }

# Helpers de sessão/acesso
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario_logado" not in session:
            flash("Faça login para acessar esta página.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


def _carregar_permissoes_por_perfil(perfil_id: int):
    """Carrega permissões do banco para um perfil (id do perfil)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.recurso
        FROM perfil_permissao pp
        JOIN permissoes p ON p.id = pp.permissao_id
        WHERE pp.perfil_id = %s
    """, (perfil_id,))
    perms = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return perms


def has_perm(recurso: str) -> bool:
    # Fallback seguro: se não carregou permissões ainda, admin (tipo_id=1) enxerga tudo
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    return recurso in perms


def perm_required(*recursos):
    """
    Uso: @perm_required('usuarios.criar') ou múltiplos @perm_required('a','b')
    Se o usuário tiver qualquer uma das permissões listadas, passa.
    """
    def deco(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if "usuario_logado" not in session:
                flash("Faça login para acessar esta página.", "danger")
                return redirect(url_for("login"))
            perms = session.get("permissoes", [])
            if not any(r in perms for r in recursos):
                flash("Acesso negado para este recurso.", "danger")
                return redirect(url_for("home"))
            return f(*args, **kwargs)
        return wrapper
    return deco


def any_perm(*recursos) -> bool:
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    perms = set(perms)
    return any(r in perms for r in recursos)


def all_perms(*recursos) -> bool:
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    perms = set(perms)
    return all(r in perms for r in recursos)


# Disponibiliza os helpers pra TODOS os templates
app.jinja_env.globals.update(
    has_perm=has_perm,
    any_perm=any_perm,
    all_perms=all_perms,
)


# ==========
# UTILIDADES
# ==========

def get_tipos_user():
    """Carrega lista de tipos do OFS (tabela local tipos_ofs)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"codigo": row[0], "descricao": row[1]} for row in resultados]


def get_perfis():
    """Carrega lista de perfis do painel (tabela perfis)."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, nome FROM perfis ORDER BY nome")
    perfis = cur.fetchall()
    cur.close()
    conn.close()
    return perfis


# =====
# Login
# =====
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        # sempre tratar login como e-mail em minúsculas
        username = (request.form.get("username") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM usuarios WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
            # atualiza last_login
            conn = get_connection()
            cur_upd = conn.cursor()
            cur_upd.execute(
                "UPDATE usuarios SET last_login = %s WHERE id = %s",
                (datetime.now(), user["id"]),
            )
            conn.commit()
            cur_upd.close()
            conn.close()

            # guarda dados básicos na sessão
            session["usuario_id"] = user["id"]
            session["usuario_logado"] = user["username"]
            session["nome_usuario"] = user["nome"]
            session["tipo_id"] = int(user["tipo_id"]) if user.get("tipo_id") is not None else 3

            # carrega permissões do perfil
            session["permissoes"] = _carregar_permissoes_por_perfil(session["tipo_id"])

            # AUDIT: login ok
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

        # AUDIT: login falho (não registra senha)
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


# Página Home
@app.route("/")
@login_required
def home():
    # carrega lista de tipos de usuário do OFS (para telas de atualização) 1x por sessão
    if "tipos_user" not in session:
        session["tipos_user"] = get_tipos_user()
    return render_template("home.html")


# ===========================
# Atualizar userType no OFS
# ===========================

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

            # AUDIT
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

        # AUDIT: bulk
        actor = current_actor()
        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="ofs",
            action="bulk_update_user_type",
            entity_type="ofs_user",
            summary=f"Atualizou userType em lote: modo={modo}, userType={user_type}, total={len(valores)}, ok={ok}, fail={fail}",
            meta={"modo": modo, "userType": user_type, "total": len(valores), "ok": ok, "fail": fail, "itens": valores[:200]},
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


# ===========================
# Criar técnicos via CSV
# ===========================

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
            data = file.read().decode("utf-8-sig")  # trata BOM
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

        linha = 1  # +1 do header para exibir ao usuário
        for row in reader:
            linha += 1
            id_sap = (row.get("idSAP") or "").strip()
            deposito_tecnico = (row.get("depositoTecnico") or "").strip()
            # tipoDeRecurso está no CSV, mas hoje usamos fixo "TCV" na API de criação
            nome_completo = (row.get("nomeCompleto") or "").strip()
            area_tecnico = (row.get("areaDoTecnico") or "").strip()
            tipo_usuario = (row.get("tipoDeUsuario") or "").strip()
            email = (row.get("email") or "").strip()
            senha = (row.get("Senha") or "").strip()

            msg_parts = []
            rec_status = "-"
            usr_status = "-"
            dep_status = "-"

            # validações mínimas
            if not id_sap or not nome_completo or not area_tecnico or not tipo_usuario or not email or not senha:
                logs.append({
                    "linha": linha, "idSAP": id_sap, "email": email,
                    "recurso_status": rec_status, "usuario_status": usr_status,
                    "deposito_status": dep_status,
                    "msg": "Dados obrigatórios ausentes na linha."
                })
                continue

            # 1) cria recurso (PUT)
            try:
                r1 = client.create_resource(
                    id_sap=id_sap,
                    parent_resource_id=area_tecnico,
                    name=nome_completo,
                    email=email
                )
                rec_status = f"{r1.status_code}"
                r1_text = (r1.text or "") if hasattr(r1, "text") else ""
                if r1.status_code in (200, 201):
                    msg_parts.append("Recurso criado/atualizado com sucesso.")
                elif r1.status_code == 409:
                    msg_parts.append("Recurso já existia (409).")
                else:
                    msg_parts.append(f"Falha ao criar recurso: {r1.status_code} {r1_text}")

            except Exception as e:
                msg_parts.append(f"Exceção na criação do recurso: {e}")

            # 2) cria usuário (PUT) — se recurso ok ou já existia
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
                    r2_text = (r2.text or "") if hasattr(r2, "text") else ""
                    if r2.status_code in (200, 201):
                        msg_parts.append("Usuário criado/atualizado com sucesso.")
                    elif r2.status_code == 409:
                        msg_parts.append("Usuário já existia (409).")
                    else:
                        msg_parts.append(f"Falha ao criar usuário: {r2.status_code} {r2_text}")
                except Exception as e:
                    msg_parts.append(f"Exceção na criação do usuário: {e}")
            else:
                msg_parts.append("Usuário não criado pois o recurso não foi criado.")

            # 3) atualiza depósito (PATCH no recurso)
            if rec_status in ("200", "201", "409"):
                if deposito_tecnico:
                    try:
                        r3 = client.update_resource_deposito(id_sap=id_sap, deposito_tecnico=deposito_tecnico)
                        dep_status = f"{r3.status_code}"
                        r3_text = (r3.text or "") if hasattr(r3, "text") else ""
                        if r3.status_code in (200, 204):
                            msg_parts.append("Depósito atualizado com sucesso.")
                        else:
                            msg_parts.append(f"Falha ao atualizar depósito: {r3.status_code} {r3_text}")
                    except Exception as e:
                        msg_parts.append(f"Exceção no update do depósito: {e}")
                else:
                    dep_status = "-"
                    msg_parts.append("Depósito não enviado (campo vazio).")

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


# ===========================
# Trocar a própria senha
# ===========================

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
            cursor.close(); conn.close()
            flash("Usuário não encontrado.", "danger")
            return redirect(url_for("trocar_senha"))

        if not bcrypt.checkpw(senha_atual.encode(), user["password_hash"].encode()):
            cursor.close(); conn.close()
            flash("Senha atual incorreta.", "danger")
            return redirect(url_for("trocar_senha"))

        novo_hash = bcrypt.hashpw(nova_senha.encode(), bcrypt.gensalt()).decode()
        cursor.execute("UPDATE usuarios SET password_hash = %s WHERE id = %s", (novo_hash, user["id"]))
        conn.commit()
        cursor.close(); conn.close()

        # AUDIT: troca de senha (sem logar a senha)
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


# ===========================
# Criar usuário do painel
# ===========================

@app.route("/criar-usuario", methods=["GET", "POST"])
@login_required
@perm_required("usuarios.criar")
def criar_usuario():
    if request.method == "POST":
        nome = (request.form.get("nome") or "").strip()

        local = (request.form.get("username_local") or "").strip().lower()
        dominio = "@verointernet.com.br"
        username = local + dominio  # monta o e-mail final

        senha = (request.form.get("senha") or "").strip()
        confirmar = (request.form.get("confirmar") or "").strip()
        tipo_id_raw = (request.form.get("tipo_id") or "").strip()

        if not nome or not local or not senha or not confirmar or not tipo_id_raw:
            flash("Preencha todos os campos.", "danger")
            return redirect(url_for("criar_usuario"))

        # valida apenas a parte local
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

        # confirma se o perfil existe
        cur.execute("SELECT id, nome FROM perfis WHERE id = %s", (tipo_id,))
        perfil_row = cur.fetchone()
        if not perfil_row:
            cur.close(); conn.close()
            flash("Perfil informado não existe.", "danger")
            return redirect(url_for("criar_usuario"))

        cur.execute("SELECT id FROM usuarios WHERE username = %s", (username,))
        if cur.fetchone():
            cur.close(); conn.close()
            flash("Já existe um usuário com esse e-mail.", "danger")
            return redirect(url_for("criar_usuario"))

        password_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
        cur.execute(
            "INSERT INTO usuarios (nome, username, password_hash, tipo_id) VALUES (%s, %s, %s, %s)",
            (nome, username, password_hash, tipo_id)
        )
        conn.commit()
        cur.close(); conn.close()

        # AUDIT: criação de usuário do painel (não loga senha)
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

    perfis = get_perfis()
    return render_template("criar_usuario.html", perfis=perfis)


# ===========================
# Consultar usuários OFS
# ===========================

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


# ===========================
# Desativar inativos / sem login
# ===========================

@app.route("/desativar_inativos", methods=["GET", "POST"])
@login_required
@perm_required("ofs.desativar")
def desativar_inativos():
    raw_days = (request.values.get("cutoff_days") or "80").strip()
    cutoff_days = int(raw_days) if raw_days.isdigit() else 80
    only_active = request.values.get("only_active") is not None

    vencidos, meta = find_stale_users(cutoff_days=cutoff_days, only_active=only_active)

    results = []
    mode = "SIMULACAO"
    if request.method == "POST":
        apply = request.form.get("apply_changes") == "1"
        results = execute_cleanup(vencidos, apply_changes=apply)
        mode = "APLICACAO" if apply else "SIMULACAO"
        flash(f"{'Aplicado' if apply else 'Simulado'} para {len(vencidos)} usuários.", "success")

        # AUDIT: cleanup
        actor = current_actor()
        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="ofs",
            action="cleanup" if apply else "cleanup_simulation",
            entity_type="ofs_user",
            summary=f"Cleanup OFS: mode={mode}, cutoff_days={cutoff_days}, only_active={only_active}, total={len(vencidos)}",
            meta={"mode": mode, "cutoff_days": cutoff_days, "only_active": only_active, "total": len(vencidos)},
        )

    if request.values.get("export") == "1":
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"/tmp/users_vencidos_{stamp}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["login", "status", "lastLoginTime", "userType", "mainResourceId"])
            for u in vencidos:
                w.writerow([u.get("login"), u.get("status"), u.get("lastLoginTime"), u.get("userType"), u.get("mainResourceId")])
        return send_file(path, as_attachment=True, download_name=os.path.basename(path), mimetype="text/csv")

    return render_template(
        "desativar_inativos.html",
        cutoff_days=cutoff_days,
        only_active=only_active,
        vencidos=vencidos,
        results=results,
        mode=mode,
        meta=meta,
    )


# ===========================
# Gestão de perfis e permissões
# ===========================

# Gerenciar perfis e permissões
@app.route("/perfis", methods=["GET", "POST"])
@login_required
@perm_required("perfis.gerenciar")
def perfis_view():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # ---------- POST: criar, salvar, deletar ----------
    if request.method == "POST":
        acao = request.form.get("acao")
        perfil_id_raw = (request.form.get("perfil_id") or "").strip()

        # --- CRIAR PERFIL ---
        if acao == "criar":
            novo_nome = (request.form.get("novo_perfil") or "").strip()
            if not novo_nome:
                flash("Informe um nome para o novo perfil.", "danger")
                cur.close(); conn.close()
                return redirect(url_for("perfis_view"))

            slug = novo_nome.lower().strip().replace(" ", "_")

            # Gera próximo ID manualmente (MAX(id) + 1)
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 AS prox_id FROM perfis")
            row = cur.fetchone()
            prox_id = row["prox_id"] if row and "prox_id" in row else 1

            cur.execute(
                "INSERT INTO perfis (id, nome, slug) VALUES (%s, %s, %s)",
                (prox_id, novo_nome, slug),
            )
            conn.commit()

            # AUDIT: criar perfil
            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="perfis",
                action="create",
                entity_type="perfil",
                entity_id=prox_id,
                entity_ref=slug,
                summary=f"Criou perfil: {novo_nome}",
                after={"id": prox_id, "nome": novo_nome, "slug": slug},
            )

            flash("Perfil criado com sucesso.", "success")
            cur.close(); conn.close()
            return redirect(url_for("perfis_view"))

        # --- SALVAR ALTERAÇÕES DE PERFIL ---
        if acao == "salvar" and perfil_id_raw:
            try:
                perfil_id = int(perfil_id_raw)
            except ValueError:
                flash("Perfil inválido.", "danger")
                cur.close(); conn.close()
                return redirect(url_for("perfis_view"))

            nome_editado = (request.form.get("nome_perfil") or "").strip()
            ids_permissoes = request.form.getlist("permissoes[]")

            if not nome_editado:
                flash("O nome do perfil não pode ser vazio.", "danger")
                cur.close(); conn.close()
                return redirect(url_for("perfis_view", perfil_id=perfil_id))

            # BEFORE: perfil + permissões
            cur.execute("SELECT id, nome, slug FROM perfis WHERE id = %s", (perfil_id,))
            before_perfil = cur.fetchone()

            cur.execute("""
                SELECT p.recurso
                FROM perfil_permissao pp
                JOIN permissoes p ON p.id = pp.permissao_id
                WHERE pp.perfil_id = %s
                ORDER BY p.recurso
            """, (perfil_id,))
            before_perms = [r["recurso"] for r in cur.fetchall()]

            # update nome
            cur.execute(
                "UPDATE perfis SET nome = %s WHERE id = %s",
                (nome_editado, perfil_id),
            )

            # Atualiza permissões
            cur.execute("DELETE FROM perfil_permissao WHERE perfil_id = %s", (perfil_id,))
            for pid in ids_permissoes:
                try:
                    pid_int = int(pid)
                    cur.execute(
                        "INSERT INTO perfil_permissao (perfil_id, permissao_id) VALUES (%s, %s)",
                        (perfil_id, pid_int),
                    )
                except ValueError:
                    continue

            conn.commit()

            # AFTER: permissões atualizadas
            cur.execute("""
                SELECT p.recurso
                FROM perfil_permissao pp
                JOIN permissoes p ON p.id = pp.permissao_id
                WHERE pp.perfil_id = %s
                ORDER BY p.recurso
            """, (perfil_id,))
            after_perms = [r["recurso"] for r in cur.fetchall()]

            # AUDIT: update perfil
            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="perfis",
                action="update",
                entity_type="perfil",
                entity_id=perfil_id,
                entity_ref=(before_perfil or {}).get("slug"),
                summary=f"Atualizou perfil: {nome_editado}",
                before={"perfil": before_perfil, "permissoes": before_perms},
                after={"perfil": {"id": perfil_id, "nome": nome_editado, "slug": (before_perfil or {}).get("slug")},
                       "permissoes": after_perms},
            )

            flash("Perfil atualizado com sucesso.", "success")
            cur.close(); conn.close()
            return redirect(url_for("perfis_view", perfil_id=perfil_id))

        # --- DELETAR PERFIL ---
        if acao == "deletar" and perfil_id_raw:
            try:
                perfil_id = int(perfil_id_raw)
            except ValueError:
                flash("Perfil inválido.", "danger")
                cur.close(); conn.close()
                return redirect(url_for("perfis_view"))

            # BEFORE: perfil (para audit)
            cur.execute("SELECT id, nome, slug FROM perfis WHERE id = %s", (perfil_id,))
            perfil_row = cur.fetchone()

            # Verifica se há usuários usando esse perfil
            cur.execute(
                "SELECT COUNT(*) AS total FROM usuarios WHERE tipo_id = %s",
                (perfil_id,),
            )
            qtd_usuarios = cur.fetchone()["total"]

            if qtd_usuarios > 0:
                flash(
                    f"Não é possível apagar: existem {qtd_usuarios} usuário(s) usando este perfil.",
                    "danger",
                )
                cur.close(); conn.close()
                return redirect(url_for("perfis_view", perfil_id=perfil_id))

            # Remove vínculos antes de apagar
            cur.execute("DELETE FROM perfil_permissao WHERE perfil_id = %s", (perfil_id,))
            cur.execute("DELETE FROM perfis WHERE id = %s", (perfil_id,))
            conn.commit()

            # AUDIT: delete perfil
            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="perfis",
                action="delete",
                entity_type="perfil",
                entity_id=perfil_id,
                entity_ref=(perfil_row or {}).get("slug"),
                summary=f"Removeu perfil: {(perfil_row or {}).get('nome', perfil_id)}",
                before={"perfil": perfil_row},
            )

            flash("Perfil removido com sucesso.", "success")
            cur.close(); conn.close()
            return redirect(url_for("perfis_view"))

    # ---------- GET: Carregamento da tela ----------
    cur.execute("SELECT id, nome, slug FROM perfis ORDER BY nome")
    perfis = cur.fetchall()

    # Seleção do perfil atual via GET
    perfil_id = request.args.get("perfil_id", type=int)
    if not perfil_id and perfis:
        perfil_id = perfis[0]["id"]

    # Perfil selecionado
    perfil_atual = None
    if perfil_id:
        for p in perfis:
            if p["id"] == perfil_id:
                perfil_atual = p
                break

    # Carrega todas as permissões existentes
    cur.execute("SELECT id, recurso, descricao FROM permissoes ORDER BY recurso")
    permissoes = cur.fetchall()

    # Carrega permissões do perfil selecionado
    perfil_permissoes = set()
    user_count = 0

    if perfil_atual:
        cur.execute(
            "SELECT permissao_id FROM perfil_permissao WHERE perfil_id = %s",
            (perfil_atual["id"],),
        )
        perfil_permissoes = {row["permissao_id"] for row in cur.fetchall()}

        cur.execute(
            "SELECT COUNT(*) AS total FROM usuarios WHERE tipo_id = %s",
            (perfil_atual["id"],),
        )
        user_count = cur.fetchone()["total"]

    cur.close()
    conn.close()

    return render_template(
        "perfis.html",
        perfis=perfis,
        perfil_atual=perfil_atual,
        permissoes=permissoes,
        perfil_permissoes=perfil_permissoes,
        user_count=user_count,
    )


@app.route("/fechar-os-adapter", methods=["GET", "POST"])
@login_required
@perm_required("adapter.fechar_os")
def fechar_os_adapter():
    """
    Fluxo:
      - GET: exibe formulário
      - POST acao=preview: faz GET Activity + GET Resource e mostra preview (sem enviar)
      - POST acao=confirmar: envia POST para o Adapter usando dados do preview e grava log
    """
    acao = (request.form.get("acao") or "").strip().lower()
    activity_id = (request.form.get("activity_id") or "").strip()

    # Preview fica na sessão para evitar adulteração via form
    preview = session.get("adapter_preview")

    def _get_usuario_id_logado():
        # Preferência: se você já tem session["usuario_id"] no projeto
        uid = session.get("usuario_id")
        if uid:
            return int(uid)

        # Fallback: buscar por username
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM usuarios WHERE username = %s", (session.get("usuario_logado"),))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return int(row["id"]) if row else 0

    def _log_fechamento(**kwargs):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO adapter_fechamento_os_log
            (usuario_id, activity_id, resource_id, cod_atendimento, id_fechamento,
             payload_json, response_status, response_body, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            kwargs.get("usuario_id"),
            kwargs.get("activity_id"),
            kwargs.get("resource_id"),
            kwargs.get("cod_atendimento"),
            kwargs.get("id_fechamento"),
            kwargs.get("payload_json"),
            kwargs.get("response_status"),
            kwargs.get("response_body"),
            kwargs.get("error_message"),
        ))
        conn.commit()
        cur.close()
        conn.close()

    # -------- GET: tela vazia --------
    if request.method == "GET":
        # não mantém preview antigo ao entrar na página
        session.pop("adapter_preview", None)
        return render_template("fechar_os_adapter.html", stage="form", activity_id="")

    # -------- POST: preview --------
    if acao == "preview":
        if not activity_id:
            flash("Informe o ID da atividade OFS.", "danger")
            return render_template("fechar_os_adapter.html", stage="form", activity_id="")

        try:
            client = OFSClient()

            # 1) Get Activity
            atividade = client.authenticated_get(f"{client.base_url}/activities/{activity_id}")

            resource_id = atividade.get("resourceId")
            if not resource_id:
                raise ValueError("A atividade não possui resourceId.")

            cod_atendimento = atividade.get("XA_SOL_ID")
            start_time = atividade.get("startTime")
            obs = atividade.get("XA_TSK_NOT")
            id_fechamento = atividade.get("XA_SER_CLO_PRO_ADA") or atividade.get("XA_SER_CLO_IMP_ADA")

            if not cod_atendimento:
                raise ValueError("Atividade sem XA_SOL_ID (CodAtendimento).")
            if not id_fechamento:
                raise ValueError("Atividade sem XA_SER_CLO_PRO_ADA e sem XA_SER_CLO_IMP_ADA.")

            # 2) Get Resource
            recurso = client.authenticated_get(f"{client.base_url}/resources/{resource_id}")
            resource_name = recurso.get("name")
            xr_user = recurso.get("XR_USER_ADAPTER")
            xr_pass = recurso.get("XR_PASSWORD_ADAPTER")
            
            if not resource_name:
                resource_name = "Recurso sem nome"
            
            if not xr_user or not xr_pass:
                raise ValueError("Recurso sem XR_USER_ADAPTER ou XR_PASSWORD_ADAPTER.")

            # 3) Monta payload (não envia ainda)
            payload = {
                "usuario": xr_user,
                "senha": xr_pass,
                "DadosFechamento": {
                    "CodAtendimento": cod_atendimento,
                    "DataInicioAtendimento": start_time,
                    "IDFechamento": id_fechamento,
                    "IDInterface": None,
                    "JustificativaReagendamento": "null",
                    "IDMotivoReagendamento": None,
                    "ObsFechamento": obs,
                    "obsFechamentoLog": "NULL",
                    "CodTecnico": resource_id,
                    "MovimentouEquipamento": True,
                    "MovimentouMaterial": True,
                    "MovimentouEquipamentoCliente": True
                }
            }

            preview = {
                "activity_id": activity_id,
                "resource_id": str(resource_id),
                "resource_name": resource_name,   
                "xr_user": xr_user,            
                "xr_pass": xr_pass,               
                "cod_atendimento": str(cod_atendimento),
                "start_time": start_time,
                "id_fechamento": str(id_fechamento),
                "obs": obs,
                "payload": payload,  # fica na sessão
            }
            session["adapter_preview"] = preview

            return render_template("fechar_os_adapter.html", stage="preview", preview=preview, activity_id=activity_id)

        except Exception as e:
            session.pop("adapter_preview", None)
            flash(f"Erro ao montar preview: {e}", "danger")
            return render_template("fechar_os_adapter.html", stage="form", activity_id=activity_id)

    # -------- POST: confirmar envio --------
    if acao == "confirmar":
        if not preview:
            flash("Preview expirado. Gere o preview novamente.", "danger")
            return render_template("fechar_os_adapter.html", stage="form", activity_id="")

        try:
            close_url = os.getenv("URL_CLOSE_ADAPTER")
            auth_ada = os.getenv("AUTH_ADA")     # valor depois de "Basic "
            cookie_ada = os.getenv("COOKIE_ADA")

            if not close_url:
                raise RuntimeError("URL_CLOSE_ADAPTER não configurado no .env.")
            if not auth_ada:
                raise RuntimeError("AUTH_ADA não configurado no .env.")
            if not cookie_ada:
                raise RuntimeError("COOKIE_ADA não configurado no .env.")

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {auth_ada}",
                "Cookie": cookie_ada,
            }

            payload = preview["payload"]
            resp = requests.post(close_url, json=payload, headers=headers, timeout=30)

            usuario_id = _get_usuario_id_logado()

            _log_fechamento(
                usuario_id=usuario_id,
                activity_id=preview.get("activity_id"),
                resource_id=preview.get("resource_id"),
                cod_atendimento=preview.get("cod_atendimento"),
                id_fechamento=preview.get("id_fechamento"),
                payload_json=json.dumps(payload, ensure_ascii=False),
                response_status=resp.status_code,
                response_body=(resp.text or "")[:65000],
                error_message=None,
            )
            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="adapter",
                action="close_os",
                entity_type="activity",
                entity_ref=preview.get("activity_id"),
                summary=f"Fechou OS via Adapter: activityId={preview.get('activity_id')} HTTP={resp.status_code}",
                meta={
                    "activity_id": preview.get("activity_id"),
                    "resource_id": preview.get("resource_id"),
                    "resource_name": preview.get("resource_name"),
                    "cod_atendimento": preview.get("cod_atendimento"),
                    "id_fechamento": preview.get("id_fechamento"),
                    "status_code": resp.status_code,
                },
            )

            # limpa preview após tentativa
            session.pop("adapter_preview", None)

            if 200 <= resp.status_code < 300:
                flash("Fechamento enviado com sucesso para o Adapter.", "success")
            else:
                flash(f"Adapter retornou erro HTTP {resp.status_code}.", "danger")

            return render_template(
                "fechar_os_adapter.html",
                stage="result",
                result={"status_code": resp.status_code, "body": (resp.text or "")[:5000]},
                activity_id=preview.get("activity_id"),
            )

        except Exception as e:
            # tenta logar erro também
            try:
                usuario_id = _get_usuario_id_logado()
                _log_fechamento(
                    usuario_id=usuario_id,
                    activity_id=(preview or {}).get("activity_id") or activity_id,
                    resource_id=(preview or {}).get("resource_id"),
                    cod_atendimento=(preview or {}).get("cod_atendimento"),
                    id_fechamento=(preview or {}).get("id_fechamento"),
                    payload_json=json.dumps((preview or {}).get("payload") or {}, ensure_ascii=False),
                    response_status=None,
                    response_body=None,
                    error_message=str(e),
                )
            except Exception:
                pass

            flash(f"Erro ao enviar fechamento: {e}", "danger")
            return render_template("fechar_os_adapter.html", stage="preview", preview=preview, activity_id=preview.get("activity_id"))

    # fallback
    flash("Ação inválida.", "danger")
    return redirect(url_for("fechar_os_adapter"))
@app.route("/logs", methods=["GET"])
@login_required
@perm_required("logs.visualizar")
def logs_view():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # filtros
    user = request.args.get("user", "").strip()
    module = request.args.get("module", "").strip()
    action = request.args.get("action", "").strip()
    q = request.args.get("q", "").strip()
    date_ini = request.args.get("date_ini", "").strip()
    date_fim = request.args.get("date_fim", "").strip()
    export = request.args.get("export") == "1"

    base_query = """
        FROM audit_log
        WHERE 1=1
    """
    params = []

    if user:
        base_query += " AND actor_username LIKE %s"
        params.append(f"%{user}%")

    if module:
        base_query += " AND module = %s"
        params.append(module)

    if action:
        base_query += " AND action = %s"
        params.append(action)

    if q:
        base_query += " AND (summary LIKE %s OR entity_ref LIKE %s)"
        params.extend([f"%{q}%", f"%{q}%"])

    if date_ini:
        base_query += " AND created_at >= %s"
        params.append(f"{date_ini} 00:00:00")

    if date_fim:
        base_query += " AND created_at <= %s"
        params.append(f"{date_fim} 23:59:59")

    # -------- EXPORT CSV --------
    if export:
        cur.execute(
            f"""
            SELECT
                created_at,
                actor_username,
                module,
                action,
                summary,
                entity_type,
                entity_ref
            {base_query}
            ORDER BY created_at DESC
            """,
            tuple(params),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # CSV em texto
        text_buffer = StringIO()
        writer = csv.writer(text_buffer)
        writer.writerow([
            "data_hora",
            "usuario",
            "modulo",
            "acao",
            "resumo",
            "tipo_entidade",
            "referencia",
        ])

        for r in rows:
            writer.writerow([
                r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else "",
                r.get("actor_username") or "",
                r.get("module") or "",
                r.get("action") or "",
                r.get("summary") or "",
                r.get("entity_type") or "",
                r.get("entity_ref") or "",
            ])

        # Converte para bytes e envia
        csv_bytes = text_buffer.getvalue().encode("utf-8-sig")  # UTF-8 com BOM (excel-friendly)
        output = BytesIO(csv_bytes)

        return send_file(
            output,
            mimetype="text/csv; charset=utf-8",
            as_attachment=True,
            download_name=f"audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )

    # -------- HTML --------
    cur.execute(
        f"""
        SELECT
            id,
            created_at,
            actor_username,
            module,
            action,
            summary,
            entity_type,
            entity_ref
        {base_query}
        ORDER BY created_at DESC
        LIMIT 500
        """,
        tuple(params),
    )
    logs = cur.fetchall()

    # filtros auxiliares
    cur.execute("SELECT DISTINCT module FROM audit_log ORDER BY module")
    modules = [r["module"] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT action FROM audit_log ORDER BY action")
    actions = [r["action"] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return render_template(
        "logs.html",
        logs=logs,
        modules=modules,
        actions=actions,
        filtros={
            "user": user,
            "module": module,
            "action": action,
            "q": q,
            "date_ini": date_ini,
            "date_fim": date_fim,
        },
    )


# Listar usuários de um perfil específico
@app.route("/usuarios-por-perfil/<int:perfil_id>")
@login_required
@perm_required("perfis.gerenciar")
def usuarios_por_perfil(perfil_id):
    # Redireciona para a nova tela única de usuários, já com filtro aplicado
    return redirect(url_for("usuarios_painel", perfil_id=perfil_id))

@app.route("/usuarios", methods=["GET"])
@login_required
@perm_required("usuarios.criar")  # ou "perfis.gerenciar" se quiser restringir ainda mais
def usuarios_painel():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # carregar perfis para o filtro
    cur.execute("SELECT id, nome FROM perfis ORDER BY nome")
    perfis = cur.fetchall()

    perfil_id = request.args.get("perfil_id", type=int)

    query = """
        SELECT u.id,
               u.nome,
               u.username,
               p.nome AS perfil_nome,
               u.last_login
        FROM usuarios u
        LEFT JOIN perfis p ON p.id = u.tipo_id
    """
    params = []
    if perfil_id:
        query += " WHERE u.tipo_id = %s"
        params.append(perfil_id)

    query += " ORDER BY u.nome"

    cur.execute(query, tuple(params))
    usuarios = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "usuarios_painel.html",
        usuarios=usuarios,
        perfis=perfis,
        perfil_id_selecionado=perfil_id,
    )
def registrar_atividade_usuario():
    """Atualiza a tabela usuarios_online com o último acesso do usuário logado."""
    usuario_id = session.get("usuario_id")
    if not usuario_id:
        return

    conn = get_connection()
    cur = conn.cursor()

    agora = datetime.now()
    # se quiser UTC, use datetime.utcnow()

    cur.execute(
        """
        INSERT INTO usuarios_online (usuario_id, last_seen)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)
        """,
        (usuario_id, agora),
    )
    conn.commit()
    cur.close()
    conn.close()

@app.route("/status-online")
@login_required
def status_online():
    conn = get_connection()
    cur = conn.cursor()

    limite = datetime.now() - timedelta(minutes=5)

    cur.execute(
        "SELECT COUNT(*) FROM usuarios_online WHERE last_seen >= %s",
        (limite,),
    )
    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        "usuarios_online": count
    })
# ===========================
# Logout
# ===========================

@app.route("/logout")
def logout():
    actor = current_actor()

    # AUDIT: logout
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
        cur.execute("DELETE FROM usuarios_online WHERE usuario_id = %s", (usuario_id,))
        conn.commit()
        cur.close()
        conn.close()

    session.clear()
    return redirect(url_for("login"))



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)