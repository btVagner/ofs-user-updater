from flask import jsonify

from core.auth import login_required
from services.online_service import obter_usuarios_online_count


def init_app(app):
    @app.route("/status-online")
    @login_required
    def status_online():
        count = obter_usuarios_online_count()
        return jsonify({
            "usuarios_online": count
        })