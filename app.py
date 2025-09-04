from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session
import requests
from datetime import datetime
import csv
from dotenv import load_dotenv
from database.connection import get_connection
import bcrypt
import os
from functools import wraps
load_dotenv()
from ofs.cleanup import find_stale_users, execute_cleanup
from ofs.client import OFSClient

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario_logado" not in session:
            flash("Faça login para acessar esta página.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


# Login
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
            session["usuario_logado"] = user["username"]
            session["nome_usuario"] = user["nome"]
            session["tipo_id"] = user["tipo_id"]

            return redirect(url_for("home"))
        else:
            flash("Usuário ou senha inválidos!", "danger")
            return redirect(url_for("login"))

    return render_template("login.html")


# Rota protegida para atualizar userType do OFS
@app.route('/')
@login_required
def home():
    if 'tipos_user' not in session:
        session['tipos_user'] = get_tipos_user()
    
    return render_template('home.html')

@app.route("/atualizar", methods=["GET", "POST"])
@login_required
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

@app.route('/atualizar-um', methods=['GET', 'POST'])
@login_required
def atualizar_um():
    tipos_user = session.get('tipos_user', [])

    if request.method == 'POST':
        resource_id = request.form.get('resource_id')
        user_type_codigo = request.form.get('user_type')

        session['ultimo_user_type'] = user_type_codigo  # salva na sessão

        from ofs.client import OFSClient
        username = os.getenv("OFS_USERNAME")
        password = os.getenv("OFS_PASSWORD")
        client = OFSClient(username, password)

        try:
            login = client.get_login_by_resource_id(resource_id)
            status, _ = client.update_user_type(login, user_type_codigo)
            flash(f"✅ Login {login} atualizado com sucesso! (Status: {status})", "success")
        except Exception as e:
            flash(f"❌ Erro ao atualizar o userType: {e}", "danger")

        return redirect(url_for('atualizar_um'))

    selected = session.pop('ultimo_user_type', '')  # remove da sessão após uso
    return render_template('atualizar_um.html', tipos=tipos_user, selected=selected)



@app.route('/atualizar-varios', methods=['GET', 'POST'])
@login_required
def atualizar_varios():
    tipos_user = session.get('tipos_user', [])

    if request.method == 'POST':
        modo = request.form.get('modo')  # "resourceId" ou "email"
        valores_raw = request.form.get('identificadores', '')
        user_type = request.form.get('user_type')

        valores = [v.strip() for v in valores_raw.split(',') if v.strip()]
        logs = []

        from ofs.client import OFSClient
        username = os.getenv("OFS_USERNAME")
        password = os.getenv("OFS_PASSWORD")
        client = OFSClient(username, password)

        for item in valores:
            try:
                if modo == 'email':
                    login = item
                else:
                    login = client.get_login_by_resource_id(item)

                status, _ = client.update_user_type(login, user_type)
                logs.append(f"✅ {item} → {login} atualizado com sucesso (Status: {status})")
            except Exception as e:
                logs.append(f"❌ {item} → Erro: {e}")

        session['log_varios'] = logs  # Armazenar no session temporariamente

        return redirect(url_for('log_varios'))

    return render_template('atualizar_varios.html', tipos=tipos_user)

@app.route('/log-varios')
@login_required
def log_varios():
    logs = session.pop('log_varios', [])
    return render_template('log_varios.html', logs=logs)


def get_tipos_user():
    from database.connection import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{'codigo': row[0], 'descricao': row[1]} for row in resultados]

@app.route("/logout")
def logout():
    session.pop("usuario_logado", None)
    flash("Você saiu da sessão.", "success")
    session.clear()

    return redirect(url_for("login"))

@app.route("/consultar-usuarios")
@login_required
def consultar_usuarios():
    client = OFSClient()
    usuarios_raw = client.get_usuarios()

    usuarios_filtrados = []
    for u in usuarios_raw:
        usuarios_filtrados.append({
            'name': u.get('name', '-'),
            'userType': u.get('userType', '-'),
            'status': u.get('status', '-'),
            'login': u.get('login', '-'),
            'code_sap': u.get('XU_CODE_SAP', '-'),
            'lastLoginTime': u.get('lastLoginTime', '-'),
            
        })

    ativos = sum(1 for u in usuarios_filtrados if u['status'] == 'active')

    return render_template(
        "consultar_usuarios.html",
        usuarios=usuarios_filtrados,
        total_ativos=ativos
    )


@app.route("/desativar_inativos", methods=["GET", "POST"])
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

# Iniciar servidor Flask
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
