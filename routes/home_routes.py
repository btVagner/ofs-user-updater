from flask import jsonify, render_template, session

from core.auth import current_actor, has_perm, login_required
from services.dashboard_operacional_service import (
    get_dashboard_snapshot_status,
    get_or_start_dashboard_snapshot,
    unlock_dashboard_snapshot,
)
from services.tipos_ofs_service import get_tipos_user


def init_app(app):
    @app.route("/")
    @login_required
    def home():
        if not has_perm("dashboard.operacional_acessar"):
            if "tipos_user" not in session:
                session["tipos_user"] = get_tipos_user()
            return render_template("home.html")

        snapshot = get_or_start_dashboard_snapshot()
        actor = current_actor()

        return render_template(
            "dashboard_operacional.html",
            snapshot=snapshot,
            payload=snapshot.get("payload") or {},
            is_admin=actor.get("tipo_id") == 1,
        )

    @app.route("/menu")
    @login_required
    def menu():
        if "tipos_user" not in session:
            session["tipos_user"] = get_tipos_user()
        return render_template("home.html")

    @app.route("/dashboard/status")
    @login_required
    def dashboard_status():
        if not has_perm("dashboard.operacional_acessar"):
            return jsonify({
                "ok": False,
                "error": "Acesso negado para este recurso.",
            }), 403

        return jsonify(get_dashboard_snapshot_status())

    @app.route("/dashboard/destravar", methods=["POST"])
    @login_required
    def dashboard_unlock():
        actor = current_actor()

        if actor.get("tipo_id") != 1:
            return jsonify({
                "ok": False,
                "error": "Apenas administrador pode destravar a atualização do dashboard.",
            }), 403

        result = unlock_dashboard_snapshot()
        return jsonify(result), 200