from datetime import date

from flask import jsonify, render_template, request

from core.auth import current_actor, perm_required
from services.bi_activities_service import (
    BIActivitiesError,
    get_last_job_summary,
    list_available_activity_types,
    list_recent_jobs,
    run_collection,
    sync_close_reason_map,
)


def init_app(app):
    @app.get("/bi-activities")
    @perm_required("bi_activities.acessar")
    def bi_activities_page():
        """
        Tela operacional inicial do módulo BI Activities.
        """

        activity_types = list_available_activity_types()

        return render_template(
            "bi_activities.html",
            today=date.today().strftime("%Y-%m-%d"),
            activity_types=activity_types,
        )

    @app.post("/bi-activities/coletar")
    @perm_required("bi_activities.executar_coleta")
    def bi_activities_coletar():
        """
        Executa uma coleta manual das atividades OFS para a base BI.
        """

        payload = request.get_json(silent=True) or {}

        date_from = payload.get("dateFrom") or payload.get("date_from")
        date_to = payload.get("dateTo") or payload.get("date_to")
        resources = payload.get("resources") or "02"
        statuses = payload.get("statuses")
        activity_types = payload.get("activityTypes") or payload.get("activity_types")

        if not date_from:
            return jsonify({
                "ok": False,
                "error": "Informe dateFrom."
            }), 400

        if not date_to:
            return jsonify({
                "ok": False,
                "error": "Informe dateTo."
            }), 400

        if statuses is not None and not isinstance(statuses, list):
            return jsonify({
                "ok": False,
                "error": "statuses deve ser uma lista."
            }), 400

        if activity_types is not None and not isinstance(activity_types, list):
            return jsonify({
                "ok": False,
                "error": "activityTypes deve ser uma lista."
            }), 400

        actor = current_actor()

        try:
            result = run_collection(
                date_from=date_from,
                date_to=date_to,
                statuses=statuses,
                resources=resources,
                activity_types=activity_types,
                created_by_user_id=actor.get("id"),
                created_by_username=actor.get("username"),
            )

            return jsonify(result), 200

        except BIActivitiesError as exc:
            return jsonify({
                "ok": False,
                "error": str(exc)
            }), 400

        except Exception as exc:
            return jsonify({
                "ok": False,
                "error": "Erro inesperado ao executar coleta BI.",
                "detail": str(exc)
            }), 500

    @app.get("/bi-activities/jobs")
    @perm_required("bi_activities.acessar")
    def bi_activities_jobs():
        """
        Lista as últimas execuções da coleta BI.
        """

        try:
            limit = request.args.get("limit", 20)
            jobs = list_recent_jobs(limit=limit)

            return jsonify({
                "ok": True,
                "jobs": jobs,
            }), 200

        except Exception as exc:
            return jsonify({
                "ok": False,
                "error": "Erro ao listar jobs BI.",
                "detail": str(exc),
            }), 500

    @app.get("/bi-activities/summary")
    @perm_required("bi_activities.acessar")
    def bi_activities_summary():
        """
        Retorna resumo operacional do último job BI.
        """

        try:
            summary = get_last_job_summary()

            return jsonify({
                "ok": True,
                "summary": summary,
            }), 200

        except Exception as exc:
            return jsonify({
                "ok": False,
                "error": "Erro ao carregar resumo BI.",
                "detail": str(exc),
            }), 500

    @app.post("/bi-activities/close-reasons/sync")
    @perm_required("bi_activities.executar_coleta")
    def bi_activities_sync_close_reasons():
        """
        Atualiza a tabela local de motivos de fechamento a partir do OFS Metadata.
        """

        try:
            result = sync_close_reason_map()

            return jsonify(result), 200

        except BIActivitiesError as exc:
            return jsonify({
                "ok": False,
                "error": str(exc),
            }), 400

        except Exception as exc:
            return jsonify({
                "ok": False,
                "error": "Erro inesperado ao atualizar motivos de fechamento.",
                "detail": str(exc),
            }), 500