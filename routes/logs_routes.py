from flask import render_template, request, send_file, url_for, flash, redirect, flash, redirect

from core.auth import login_required, perm_required, current_actor
from database.audit import audit_log
from services.logs_service import (
    LOGS_PER_PAGE_OPTIONS,
    build_log_filters,
    export_audit_logs_csv,
    export_logs_dashboard_xlsx,
    fetch_audit_logs,
    get_logs_dashboard_data,
    list_filter_options,
    validate_export_period,
)


def _page_url(endpoint, page):
    args = request.args.to_dict(flat=True)
    args["page"] = page
    args.pop("export", None)
    return url_for(endpoint, **args)


def _current_args_without_export():
    args = request.args.to_dict(flat=True)
    args.pop("export", None)
    return args


def init_app(app):

    @app.route("/logs", methods=["GET"])
    @login_required
    @perm_required("logs.visualizar")
    def logs_view():
        filtros = build_log_filters(request.args, dashboard=False)

        page = request.args.get("page", 1)
        per_page = request.args.get("per_page", 50)
        export = request.args.get("export") == "1"

        if export:
            valid_export, export_error = validate_export_period(filtros)
            if not valid_export:
                flash(export_error, "danger")
                args = _current_args_without_export()
                return redirect(url_for("logs_view", **args))

            output, filename, total_rows = export_audit_logs_csv(filtros)

            actor = current_actor()
            try:
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="logs",
                    action="export_audit_log_csv",
                    entity_type="audit_log",
                    summary="Exportou CSV do log do sistema",
                    meta={
                        "filters": filtros,
                        "total_rows": total_rows,
                    },
                )
            except Exception:
                pass

            return send_file(
                output,
                mimetype="text/csv; charset=utf-8",
                as_attachment=True,
                download_name=filename,
            )

        result = fetch_audit_logs(
            filtros,
            page=page,
            per_page=per_page,
        )

        modules, actions = list_filter_options()

        prev_page_url = _page_url("logs_view", result["page"] - 1) if result["has_prev"] else None
        next_page_url = _page_url("logs_view", result["page"] + 1) if result["has_next"] else None

        export_args = _current_args_without_export()
        export_args["export"] = 1
        export_url = url_for("logs_view", **export_args)

        dashboard_args = {
            "date_ini": filtros.get("date_ini") or "",
            "date_fim": filtros.get("date_fim") or "",
            "user": filtros.get("user") or "",
            "module": filtros.get("module") or "",
            "action": filtros.get("action") or "",
            "q": filtros.get("q") or "",
        }
        dashboard_url = url_for("logs_dashboard", **dashboard_args)

        return render_template(
            "logs.html",
            logs=result["logs"],
            modules=modules,
            actions=actions,
            filtros=filtros,
            per_page_options=LOGS_PER_PAGE_OPTIONS,
            pagination={
                "total": result["total"],
                "page": result["page"],
                "per_page": result["per_page"],
                "total_pages": result["total_pages"],
                "page_start": result["page_start"],
                "page_end": result["page_end"],
                "prev_page_url": prev_page_url,
                "next_page_url": next_page_url,
            },
            export_url=export_url,
            dashboard_url=dashboard_url,
        )

    @app.route("/logs/dashboard", methods=["GET"])
    @login_required
    @perm_required("logs.visualizar")
    def logs_dashboard():
        filtros = build_log_filters(request.args, dashboard=True)
        data = get_logs_dashboard_data(filtros)
        modules, actions = list_filter_options()

        export_url = url_for(
            "logs_dashboard_exportar",
            date_ini=filtros.get("date_ini") or "",
            date_fim=filtros.get("date_fim") or "",
            user=filtros.get("user") or "",
            module=filtros.get("module") or "",
            action=filtros.get("action") or "",
            q=filtros.get("q") or "",
        )

        return render_template(
            "logs_dashboard.html",
            filtros=filtros,
            modules=modules,
            actions=actions,
            data=data,
            export_url=export_url,
        )

    @app.route("/logs/dashboard/exportar", methods=["GET"])
    @login_required
    @perm_required("logs.visualizar")
    def logs_dashboard_exportar():
        filtros = build_log_filters(request.args, dashboard=True)

        valid_export, export_error = validate_export_period(filtros)
        if not valid_export:
            flash(export_error, "danger")
            return redirect(url_for(
                "logs_dashboard",
                date_ini=filtros.get("date_ini") or "",
                date_fim=filtros.get("date_fim") or "",
                user=filtros.get("user") or "",
                module=filtros.get("module") or "",
                action=filtros.get("action") or "",
                q=filtros.get("q") or "",
            ))

        output, filename = export_logs_dashboard_xlsx(filtros)

        actor = current_actor()
        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="logs",
                action="export_logs_dashboard_xlsx",
                entity_type="audit_log",
                summary="Exportou XLSX do dashboard de logs",
                meta={
                    "filters": filtros,
                    "filename": filename,
                },
            )
        except Exception:
            pass

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
