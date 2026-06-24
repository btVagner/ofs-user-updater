from flask import jsonify, session

from core.auth import login_required
from services.online_service import obter_usuarios_online_count, listar_usuarios_online


def init_app(app):
    @app.route("/status-online")
    @login_required
    def status_online():
        count = obter_usuarios_online_count()
        return jsonify({
            "usuarios_online": count
        })

    @app.route("/status-online/usuarios")
    @login_required
    def status_online_usuarios():
        if session.get("tipo_id") != 1:
            return jsonify({
                "ok": False,
                "error": "Apenas administrador pode visualizar usuários online.",
            }), 403

        return jsonify({
            "ok": True,
            "usuarios": listar_usuarios_online(),
        })