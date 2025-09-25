from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session
import requests
from datetime import datetime
import csv
from dotenv import load_dotenv
from database.connection import get_connection
import bcrypt
import os
from functools import wraps
from io import StringIO

load_dotenv()

from ofs.cleanup import find_stale_users, execute_cleanup
from ofs.client import OFSClient

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")


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
    perms = session.get("permissoes", [])
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

def has_perm(recurso: str) -> bool:
    # Fallback seguro: se não carregou permissões ainda, admin (tipo_id=1) enxerga tudo
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    return recurso in perms

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
# =====
# Login
# =====
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM usuarios WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
            # guarda dados básicos
            session["usuario_logado"] = user["username"]
            session["nome_usuario"] = user["nome"]
            session["tipo_id"] = int(user["tipo_id"]) if user.get("tipo_id") is not None else 3

            # carrega permissões do perfil
            session["permissoes"] = _carregar_permissoes_por_perfil(session["tipo_id"])

            return redirect(url_for("home"))
        else:
            flash("Usuário ou senha inválidos!", "danger")
            return redirect(url_for("login"))

    return render_template("login.html")



# Página Home

@app.route("/")
@login_required
def home():
    # carrega lista de tipos de usuário do OFS (para telas de atualização) 1x por sessão
    if "tipos_user" not in session:
        session["tipos_user"] = get_tipos_user()
    return render_template("home.html")



# Atualizar userType (menu com opções de telas)

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



# Atualizar userType de 1 técnico (por resource_id)

@app.route("/atualizar-um", methods=["GET", "POST"])
@login_required
@perm_required("ofs.atualizar_tipo")
def atualizar_um():
    tipos_user = session.get("tipos_user", [])

    if request.method == "POST":
        resource_id = request.form.get("resource_id")
        user_type_codigo = request.form.get("user_type")

        session["ultimo_user_type"] = user_type_codigo  # salva para pré-selecionar após refresh

        username = os.getenv("OFS_USERNAME")
        password = os.getenv("OFS_PASSWORD")
        client = OFSClient(username, password)

        try:
            login = client.get_login_by_resource_id(resource_id)
            status, _ = client.update_user_type(login, user_type_codigo)
            flash(f"✅ Login {login} atualizado com sucesso! (Status: {status})", "success")
        except Exception as e:
            flash(f"❌ Erro ao atualizar o userType: {e}", "danger")

        return redirect(url_for("atualizar_um"))

    selected = session.pop("ultimo_user_type", "")
    return render_template("atualizar_um.html", tipos=tipos_user, selected=selected)



# Atualizar userType de vários técnicos (email/id)

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

        for item in valores:
            try:
                if modo == "email":
                    login = item
                else:
                    login = client.get_login_by_resource_id(item)

                status, _ = client.update_user_type(login, user_type)
                logs.append(f"✅ {item} → {login} atualizado com sucesso (Status: {status})")
            except Exception as e:
                logs.append(f"❌ {item} → Erro: {e}")

        session["log_varios"] = logs  # exibir em página separada
        return redirect(url_for("log_varios"))

    return render_template("atualizar_varios.html", tipos=tipos_user)


@app.route("/log-varios")
@login_required
@perm_required("ofs.atualizar_tipo")
def log_varios():
    logs = session.pop("log_varios", [])
    return render_template("log_varios.html", logs=logs)


# Criar técnicos via CSV (recurso + usuário)
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

    return render_template("criar_tecnicos.html", logs=logs)


# Trocar a própria senha
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

        flash("Senha alterada com sucesso!", "success")
        return redirect(url_for("home"))

    return render_template("trocar_senha.html")


# Criar usuário do painel
@app.route("/criar-usuario", methods=["GET", "POST"])
@login_required
@perm_required("usuarios.criar")
def criar_usuario():
    if request.method == "POST":
        nome = (request.form.get("nome") or "").strip()
        username = (request.form.get("username") or "").strip()
        senha = (request.form.get("senha") or "").strip()
        confirmar = (request.form.get("confirmar") or "").strip()
        tipo_id_raw = (request.form.get("tipo_id") or "").strip()

        if not nome or not username or not senha or not confirmar or not tipo_id_raw:
            flash("Preencha todos os campos.", "danger")
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
        cur.execute("SELECT id FROM usuarios WHERE username = %s", (username,))
        if cur.fetchone():
            cur.close(); conn.close()
            flash("Já existe um usuário com esse login.", "danger")
            return redirect(url_for("criar_usuario"))

        password_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
        cur.execute(
            "INSERT INTO usuarios (nome, username, password_hash, tipo_id) VALUES (%s, %s, %s, %s)",
            (nome, username, password_hash, tipo_id)
        )
        conn.commit()
        cur.close(); conn.close()

        flash("Usuário criado com sucesso!", "success")
        return redirect(url_for("criar_usuario"))

    return render_template("criar_usuario.html")


# Consultar usuários OFS
# Consultar usuários OFS
@app.route("/consultar-usuarios")
@login_required
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

# Desativar inativos / sem login (preview/run)
@app.route("/desativar_inativos", methods=["GET", "POST"])
@login_required
@perm_required("ofs.desativar")
def desativar_inativos():
    # robusto contra campo vazio
    raw_days = (request.values.get("cutoff_days") or "80").strip()
    cutoff_days = int(raw_days) if raw_days.isdigit() else 80
    # checkbox: True só se veio marcado
    only_active = request.values.get("only_active") is not None

    vencidos, meta = find_stale_users(cutoff_days=cutoff_days, only_active=only_active)

    results = []
    mode = "SIMULACAO"
    if request.method == "POST":
        apply = request.form.get("apply_changes") == "1"
        results = execute_cleanup(vencidos, apply_changes=apply)
        mode = "APLICACAO" if apply else "SIMULACAO"
        flash(f"{'Aplicado' if apply else 'Simulado'} para {len(vencidos)} usuários.", "success")

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


# Utilidades gerais
def get_tipos_user():
    """Carrega lista de tipos do OFS (tabela local tipos_ofs)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"codigo": row[0], "descricao": row[1]} for row in resultados]


@app.route("/logout")
def logout():
    session.pop("usuario_logado", None)
    flash("Você saiu da sessão.", "success")
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
