from flask import render_template
from core.auth import login_required


def init_app(app):

    @app.route("/ofs/erros-tratativas-dashboards", methods=["GET"])
    @login_required
    def ofs_erros_tratativas_dashboards():
        return render_template("ofs_erros_tratativas_dashboards.html")