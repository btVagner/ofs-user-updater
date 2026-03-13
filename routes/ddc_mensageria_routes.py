from flask import render_template, request, jsonify, session
from core.auth import login_required, perm_required

from services.ddc_mensageria_service import (
    DDCMensageriaError,
    get_fixed_event_option,
    send_single_ddc,
    start_massive_job,
    get_job_status,
)


def init_app(app):
    @app.route("/ddc/mensageria", methods=["GET"])
    @login_required
    @perm_required("ddc.mensageria")
    def ddc_mensageria():
        return render_template(
            "ddc_mensageria/ddc_mensageria.html",
            fixed_event_option=get_fixed_event_option(),
        )

    @app.route("/ddc/mensageria/enviar-unico", methods=["POST"])
    @login_required
    @perm_required("ddc.mensageria")
    def ddc_mensageria_enviar_unico():
        try:
            data = request.get_json(silent=True) or {}
            activity_id = str(data.get("activity_id") or "").strip()

            result = send_single_ddc(activity_id=activity_id)
            return jsonify(result), (200 if result["success"] else 400)

        except DDCMensageriaError as e:
            return jsonify({
                "success": False,
                "message": str(e),
            }), 400
        except Exception as e:
            return jsonify({
                "success": False,
                "message": f"Erro interno ao enviar OS: {str(e)}",
            }), 500

    @app.route("/ddc/mensageria/massivo/iniciar", methods=["POST"])
    @login_required
    @perm_required("ddc.mensageria")
    def ddc_mensageria_massivo_iniciar():
        try:
            data = request.get_json(silent=True) or {}
            ids = data.get("ids") or []
            usuario_id = session.get("usuario_id")

            result = start_massive_job(usuario_id=usuario_id, ids=ids)
            return jsonify(result), 200

        except DDCMensageriaError as e:
            return jsonify({
                "success": False,
                "message": str(e),
            }), 400
        except Exception as e:
            return jsonify({
                "success": False,
                "message": f"Erro interno ao iniciar job massivo: {str(e)}",
            }), 500

    @app.route("/ddc/mensageria/massivo/status/<job_id>", methods=["GET"])
    @login_required
    @perm_required("ddc.mensageria")
    def ddc_mensageria_massivo_status(job_id):
        try:
            result = get_job_status(job_id)
            return jsonify({
                "success": True,
                **result
            }), 200

        except DDCMensageriaError as e:
            return jsonify({
                "success": False,
                "message": str(e),
            }), 404
        except Exception as e:
            return jsonify({
                "success": False,
                "message": f"Erro interno ao consultar job: {str(e)}",
            }), 500