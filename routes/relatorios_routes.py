import os
import requests

from flask import render_template, request, jsonify, send_file, flash, redirect, url_for

from core.auth import login_required, perm_required, current_actor, has_perm
from database.audit import audit_log
from services.ofs_os_report_service import (
    RESOURCE_TYPES,
    STATUS_OPTIONS,
    discard_job,
    get_xlsx_path,
    list_activity_types,
    list_report_field_choices,
    list_resources_grouped,
    read_job_status,
    start_report_job,
    sync_task_type_map,
    validate_report_payload,
    start_resource_sync_job,
    read_resource_sync_job_status,
)


def init_app(app):
    def _resource_sync_base_dir():
        return os.path.join(app.instance_path, "reports", "ofs_resources_sync")

    def _reports_base_dir():
        return os.path.join(app.instance_path, "reports", "ofs_os")

    @app.route("/relatorios", methods=["GET"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios():
        """
        Central leve de relatórios.
        Não carrega recursos/campos pesados para evitar lentidão.
        """

        return render_template("relatorios.html")

    @app.route("/relatorios/ofs-os", methods=["GET"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_ofs_os_page():
        """
        Página dedicada da extração de OS do OFS.
        Aqui ficam os filtros, recursos, campos e execução do relatório.
        """

        resources_grouped = list_resources_grouped()
        can_view_extra_fields = has_perm("relatorios.campos_extras")

        return render_template(
            "relatorios_ofs_os.html",
            field_choices=list_report_field_choices(
                can_view_extra_fields=can_view_extra_fields,
            ),
            resource_types=RESOURCE_TYPES,
            resources_grouped=resources_grouped,
            status_options=STATUS_OPTIONS,
            activity_types=list_activity_types(),
            can_update_resources=has_perm("relatorios.recursos_atualizar"),
        )

    @app.route("/relatorios/recursos/atualizar/iniciar", methods=["POST"])
    @login_required
    @perm_required("relatorios.recursos_atualizar")
    def relatorios_recursos_atualizar_iniciar():
        actor = current_actor()

        try:
            job_id, active_lock = start_resource_sync_job(
                base_dir=_resource_sync_base_dir(),
                actor=actor,
            )

            if not job_id:
                return jsonify({
                    "ok": False,
                    "alreadyRunning": True,
                    "error": "Já existe uma atualização da lista de recursos em andamento.",
                    "activeJob": active_lock or {},
                }), 409

            return jsonify({
                "ok": True,
                "jobId": job_id,
                "message": "Atualização da lista de recursos iniciada em segundo plano.",
            }), 202

        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"Erro ao iniciar atualização da lista de recursos: {e}",
            }), 500

    @app.route("/relatorios/recursos/atualizar/status/<job_id>", methods=["GET"])
    @login_required
    @perm_required("relatorios.recursos_atualizar")
    def relatorios_recursos_atualizar_status(job_id):
        job_id = str(job_id or "").strip()

        if not job_id:
            return jsonify({"ok": False, "error": "job_id inválido"}), 400

        status = read_resource_sync_job_status(_resource_sync_base_dir(), job_id)

        if not status:
            return jsonify({"ok": False, "error": "Atualização não encontrada."}), 404

        return jsonify({
            "ok": True,
            "job": status,
        }), 200

    @app.route("/relatorios/task-types/sync", methods=["POST"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_task_types_sync():
        actor = current_actor()

        try:
            result = sync_task_type_map(actor=actor)

            return jsonify(result), 200

        except requests.HTTPError as e:
            response = getattr(e, "response", None)
            detail = ""

            if response is not None:
                try:
                    detail = response.text[:3000]
                except Exception:
                    detail = ""

            return jsonify({
                "ok": False,
                "error": "Erro HTTP ao atualizar tipos de tarefa do OFS.",
                "detail": detail or str(e),
            }), 500

        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"Erro ao atualizar tipos de tarefa: {e}",
            }), 500

    @app.route("/relatorios/ofs-os/iniciar", methods=["POST"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_ofs_os_iniciar():
        payload = request.get_json(silent=True) or {}

        try:
            config = validate_report_payload(
                payload,
                can_view_extra_fields=has_perm("relatorios.campos_extras"),
            )
            job_id = start_report_job(
                base_dir=_reports_base_dir(),
                actor=current_actor(),
                config=config,
            )

            return jsonify({
                "ok": True,
                "jobId": job_id,
            }), 202

        except ValueError as e:
            return jsonify({
                "ok": False,
                "error": str(e),
            }), 400

        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"Erro ao iniciar extração: {e}",
            }), 500

    @app.route("/relatorios/ofs-os/status/<job_id>", methods=["GET"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_ofs_os_status(job_id):
        job_id = str(job_id or "").strip()

        if not job_id:
            return jsonify({"ok": False, "error": "job_id inválido"}), 400

        status = read_job_status(_reports_base_dir(), job_id)

        if not status:
            return jsonify({"ok": False, "error": "Extração não encontrada"}), 404

        return jsonify({
            "ok": True,
            "job": status,
        }), 200

    @app.route("/relatorios/ofs-os/download/<job_id>", methods=["GET"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_ofs_os_download(job_id):
        job_id = str(job_id or "").strip()

        status = read_job_status(_reports_base_dir(), job_id)
        if not status:
            flash("Extração não encontrada.", "danger")
            return redirect(url_for("relatorios_ofs_os_page"))

        if status.get("status") != "completed":
            flash("A extração ainda não está concluída.", "danger")
            return redirect(url_for("relatorios_ofs_os_page"))

        xlsx_path = get_xlsx_path(_reports_base_dir(), job_id)
        if not os.path.exists(xlsx_path):
            flash("Arquivo XLSX não encontrado. Será necessário extrair novamente.", "danger")
            return redirect(url_for("relatorios_ofs_os_page"))

        actor = current_actor()

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="relatorios",
                action="download_ofs_os_report",
                entity_type="report",
                entity_ref=job_id,
                summary="Baixou Relatório de OS do OFS",
                meta={
                    "filename": status.get("filename"),
                    "dateFrom": status.get("dateFrom"),
                    "dateTo": status.get("dateTo"),
                    "statuses": status.get("statuses"),
                    "resources": status.get("resources"),
                    "fields": status.get("fields"),
                    "total_rows": status.get("total_rows"),
                },
            )
        except Exception:
            pass

        return send_file(
            xlsx_path,
            as_attachment=True,
            download_name=status.get("filename") or f"relatorio_os_ofs_{job_id}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route("/relatorios/ofs-os/descartar/<job_id>", methods=["POST"])
    @login_required
    @perm_required("relatorios.acessar")
    def relatorios_ofs_os_descartar(job_id):
        job_id = str(job_id or "").strip()
        actor = current_actor()

        status = read_job_status(_reports_base_dir(), job_id)

        try:
            discard_job(_reports_base_dir(), job_id)

            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="relatorios",
                action="discard_ofs_os_report",
                entity_type="report",
                entity_ref=job_id,
                summary="Descartou Relatório de OS do OFS",
                meta=status or {},
            )

            return jsonify({"ok": True}), 200

        except Exception as e:
            return jsonify({
                "ok": False,
                "error": f"Erro ao descartar extração: {e}",
            }), 500