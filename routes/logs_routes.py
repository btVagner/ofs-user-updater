from flask import render_template, request, send_file
from datetime import datetime
from io import BytesIO, StringIO
import csv
import json

from database.connection import get_connection
from core.auth import login_required, perm_required


def init_app(app):

    @app.route("/logs", methods=["GET"])
    @login_required
    @perm_required("logs.visualizar")
    def logs_view():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        user = request.args.get("user", "").strip()
        module = request.args.get("module", "").strip()
        action = request.args.get("action", "").strip()
        q = request.args.get("q", "").strip()
        date_ini = request.args.get("date_ini", "").strip()
        date_fim = request.args.get("date_fim", "").strip()
        export = request.args.get("export") == "1"

        base_query = """
            FROM audit_log
            WHERE 1=1
        """
        params = []

        if user:
            base_query += " AND actor_username LIKE %s"
            params.append(f"%{user}%")

        if module:
            base_query += " AND module = %s"
            params.append(module)

        if action:
            base_query += " AND action = %s"
            params.append(action)

        if q:
            base_query += " AND (summary LIKE %s OR entity_ref LIKE %s)"
            params.extend([f"%{q}%", f"%{q}%"])

        if date_ini:
            base_query += " AND created_at >= %s"
            params.append(f"{date_ini} 00:00:00")

        if date_fim:
            base_query += " AND created_at <= %s"
            params.append(f"{date_fim} 23:59:59")

        if export:
            cur.execute(
                f"""
                SELECT
                    created_at,
                    actor_username,
                    module,
                    action,
                    summary,
                    entity_type,
                    entity_ref
                {base_query}
                ORDER BY created_at DESC
                """,
                tuple(params),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            text_buffer = StringIO()
            writer = csv.writer(text_buffer)
            writer.writerow([
                "data_hora",
                "usuario",
                "modulo",
                "acao",
                "resumo",
                "tipo_entidade",
                "referencia",
            ])

            for r in rows:
                writer.writerow([
                    r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else "",
                    r.get("actor_username") or "",
                    r.get("module") or "",
                    r.get("action") or "",
                    r.get("summary") or "",
                    r.get("entity_type") or "",
                    r.get("entity_ref") or "",
                ])

            csv_bytes = text_buffer.getvalue().encode("utf-8-sig")
            output = BytesIO(csv_bytes)

            return send_file(
                output,
                mimetype="text/csv; charset=utf-8",
                as_attachment=True,
                download_name=f"audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )

        cur.execute(
            f"""
            SELECT
                id,
                created_at,
                actor_username,
                module,
                action,
                summary,
                entity_type,
                entity_ref,
                api_response
            {base_query}
            ORDER BY created_at DESC
            LIMIT 500
            """,
            tuple(params),
        )
        logs = cur.fetchall()

        for r in logs:
            raw = r.get("api_response")
            if raw is None:
                r["api_response_text"] = ""
            elif isinstance(raw, (dict, list)):
                r["api_response_text"] = json.dumps(raw, ensure_ascii=False)
            else:
                r["api_response_text"] = str(raw)

            r["api_response_text"] = (r["api_response_text"] or "")[:10000]

        cur.execute("SELECT DISTINCT module FROM audit_log ORDER BY module")
        modules = [r["module"] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT action FROM audit_log ORDER BY action")
        actions = [r["action"] for r in cur.fetchall()]

        cur.close()
        conn.close()

        return render_template(
            "logs.html",
            logs=logs,
            modules=modules,
            actions=actions,
            filtros={
                "user": user,
                "module": module,
                "action": action,
                "q": q,
                "date_ini": date_ini,
                "date_fim": date_fim,
            },
        )