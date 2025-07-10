from flask import Flask, render_template, request, redirect, url_for, flash, session
from ofs.client import OFSClient
from dotenv import load_dotenv
from database.connection import get_connection
import bcrypt
import os
from functools import wraps

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")


# Decorador para proteger rotas
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario_logado" not in session:
            flash("Faça login para acessar esta página.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


# Rota de login
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
        from database.connection import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
        tipos = cursor.fetchall()
        session['tipos_user'] = [{'codigo': row[0], 'descricao': row[1]} for row in tipos]
        cursor.close()
        conn.close()
    
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
def atualizar_um():
    tipos_user = session.get('tipos_user', [])  # Pega do cache

    if request.method == 'POST':
        resource_id = request.form.get('resource_id')
        user_type = request.form.get('user_type')
        # lógica da atualização
        flash('UserType atualizado com sucesso!', 'success')
        return redirect(url_for('atualizar_um'))

    return render_template('atualizar_um.html', tipos_user=tipos_user)


@app.route('/atualizar-varios', methods=['GET', 'POST'])
def atualizar_varios():
    if request.method == 'POST':
        lista_ids = request.form.get('resource_ids')  # exemplo: "123,456,789"
        user_type = request.form.get('user_type')
        # lógica de atualização em lote aqui
        flash('UserTypes atualizados com sucesso!', 'success')
        return redirect(url_for('atualizar_varios'))
    return render_template('atualizar_varios.html')



def get_tipos_user():
    from database.connection import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in resultados]

@app.route("/logout")
def logout():
    session.pop("usuario_logado", None)
    flash("Você saiu da sessão.", "success")
    session.clear()

    return redirect(url_for("login"))


# Iniciar servidor Flask
if __name__ == "__main__":
    app.run(debug=True)
