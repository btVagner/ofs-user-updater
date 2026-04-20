from flask import render_template, session

from core.auth import login_required
from services.tipos_ofs_service import get_tipos_user


def init_app(app):
    @app.route("/")
    @login_required
    def home():
        if "tipos_user" not in session:
            session["tipos_user"] = get_tipos_user()
        return render_template("home.html")