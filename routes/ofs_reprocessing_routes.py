from flask import render_template, request, jsonify
from datetime import datetime
import threading

from database.connection import get_connection
from core.auth import login_required, perm_required, current_actor
from services.ofs_reprocessing_service import (
    fetch_reprocessing_targets,
    run_reprocess_job,
    EXCLUDED_ACTIVITY_TYPES,
)

def init_app(app):
    @app.route("/ofs/reprocessamento/cancel/<int:job_id>", methods=["POST"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing_cancel(job_id):
        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE ofs_import_jobs
                SET cancel_requested = 1,
                    message = 'Cancelamento solicitado...',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                  AND module = 'ofs.reprocessing'
                  AND status = 'running'
            """, (job_id,))
            conn.commit()

            return jsonify({"ok": True}), 200

        finally:
            cur.close()
            conn.close()
    @app.route("/ofs/reprocessamento", methods=["GET"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing():
        today = datetime.now().strftime("%Y-%m-%d")

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            placeholders_excluded = ", ".join(["%s"] * len(EXCLUDED_ACTIVITY_TYPES))

            sql = f"""
                SELECT
                    COALESCE(e.activity_type, '-') AS activity_type,
                    COUNT(DISTINCT e.activity_id) AS qtd
                FROM ofs_activities_errors e
                INNER JOIN ofs_pending_close_ng ng
                    ON e.appt_number_norm = ng.numero_ose_norm
                WHERE (
                        NULLIF(TRIM(e.ng_dispatch_message), '') IS NOT NULL
                        OR NULLIF(TRIM(e.ng_response_message), '') IS NOT NULL
                    )
                  AND COALESCE(e.activity_type, '') NOT IN ({placeholders_excluded})
                GROUP BY COALESCE(e.activity_type, '-')
                ORDER BY qtd DESC, activity_type ASC
            """

            cur.execute(sql, list(EXCLUDED_ACTIVITY_TYPES))
            activity_types = cur.fetchall()

        finally:
            cur.close()
            conn.close()

        return render_template(
            "ofs_reprocessing/ofs_reprocessing.html",
            today=today,
            activity_types=activity_types,
        )
    @app.route("/ofs/reprocessamento/preview", methods=["POST"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing_preview():
        payload = request.get_json(silent=True) or {}

        date_from = str(payload.get("dateFrom") or "").strip()
        date_to = str(payload.get("dateTo") or "").strip()
        activity_types = payload.get("activityTypes") or []
        statuses = payload.get("statuses") or []

        try:
            rows = fetch_reprocessing_targets(
                date_from=date_from,
                date_to=date_to,
                activity_types=activity_types,
                statuses=statuses,
            )

            sample = rows[:20]

            return jsonify({
                "ok": True,
                "total": len(rows),
                "sample": sample,
            }), 200

        except Exception as e:
            return jsonify({
                "ok": False,
                "error": str(e),
            }), 400

    @app.route("/ofs/reprocessamento/start", methods=["POST"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing_start():
        payload = request.get_json(silent=True) or {}

        date_from = str(payload.get("dateFrom") or "").strip()
        date_to = str(payload.get("dateTo") or "").strip()
        activity_types = payload.get("activityTypes") or []
        statuses = payload.get("statuses") or []

        try:
            targets = fetch_reprocessing_targets(
                date_from=date_from,
                date_to=date_to,
                activity_types=activity_types,
                statuses=statuses,
            )
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400

        actor = current_actor()
        username = actor.get("username") or "desconhecido"

        conn = get_connection()
        cur = conn.cursor()

        try:
            message = (
                f"Preparando reprocessamento de {len(targets)} activityIds..."
            )

            cur.execute("""
                INSERT INTO ofs_import_jobs (
                    module,
                    status,
                    progress,
                    message,
                    created_by,
                    cancel_requested
                )
                VALUES (
                    'ofs.reprocessing',
                    'running',
                    0,
                    %s,
                    %s,
                    0
                )
            """, (message, username))
            job_id = cur.lastrowid
            conn.commit()

        finally:
            cur.close()
            conn.close()

        t = threading.Thread(
            target=run_reprocess_job,
            args=(job_id, targets),
            daemon=True,
        )
        t.start()

        return jsonify({
            "ok": True,
            "jobId": job_id,
            "total": len(targets),
        }), 200

    @app.route("/ofs/reprocessamento/status/<int:job_id>", methods=["GET"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing_status(job_id):
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    id,
                    module,
                    status,
                    progress,
                    message,
                    created_at,
                    updated_at
                FROM ofs_import_jobs
                WHERE id = %s
                  AND module = 'ofs.reprocessing'
                LIMIT 1
            """, (job_id,))
            job = cur.fetchone()

            if not job:
                return jsonify({"ok": False, "error": "Job não encontrado"}), 404

            return jsonify({"ok": True, "job": job}), 200

        finally:
            cur.close()
            conn.close()

    @app.route("/ofs/reprocessamento/logs/<int:job_id>", methods=["GET"])
    @login_required
    @perm_required("ofs.reprocessing")
    def ofs_reprocessing_logs(job_id):
        limit = request.args.get("limit", default=100, type=int)

        if limit <= 0:
            limit = 100

        if limit > 500:
            limit = 500

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    id,
                    activity_id,
                    event_type,
                    status_code,
                    response_text,
                    created_at
                FROM ofs_reprocess_logs
                WHERE job_id = %s
                ORDER BY id DESC
                LIMIT %s
            """, (job_id, limit))
            rows = cur.fetchall()

            rows.reverse()

            return jsonify({
                "ok": True,
                "items": rows,
            }), 200

        finally:
            cur.close()
            conn.close()