from flask import Flask, session
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
from datetime import datetime
import os

from routes import register_routes
from services.online_service import obter_usuarios_online_count, registrar_atividade_usuario
from core.auth import (
    has_perm,
    any_perm,
    all_perms,
)

load_dotenv()

app = Flask(__name__)

APP_ROOT = os.getenv("APP_ROOT", "").strip()
if APP_ROOT:
    app.config["APPLICATION_ROOT"] = APP_ROOT

app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

register_routes(app)


@app.before_request
def atualizar_online():
    if not session.get("usuario_logado"):
        return

    try:
        agora_ts = int(datetime.now().timestamp())
        ultimo_ping = session.get("_last_online_ping", 0)

        if agora_ts - ultimo_ping >= 60:
            registrar_atividade_usuario(session.get("usuario_id"))
            session["_last_online_ping"] = agora_ts

    except Exception as e:
        print(f"[WARN] Falha ao registrar atividade do usuário: {e}")


@app.context_processor
def inject_online_count():
    count = 0
    try:
        count = obter_usuarios_online_count()
    except Exception as e:
        print(f"[WARN] Falha ao obter usuarios_online_count: {e}")

    return dict(usuarios_online_count=count)


app.jinja_env.globals.update(
    has_perm=has_perm,
    any_perm=any_perm,
    all_perms=all_perms,
)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)