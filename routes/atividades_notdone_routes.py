from flask import render_template, request, send_file, redirect, url_for, flash, jsonify
from datetime import datetime
from io import BytesIO, StringIO
import csv

from openpyxl import Workbook

from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from core.auth import login_required, perm_required, current_actor
from core.utils import xlsx_auto_width
from urllib.parse import urlencode


def init_app(app):

    @app.route("/atividades-notdone/exportar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_exportar():
        tipo = (request.form.get("tipo") or "").strip().lower()
        date_from = (request.form.get("dateFrom") or "").strip()
        date_to = (request.form.get("dateTo") or "").strip()

        if tipo not in {"clientes", "tratativas"}:
            flash("Tipo de exportação inválido.", "danger")
            return redirect(url_for("atividades_notdone"))

        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        except Exception:
            flash("Informe um período válido (De / Até).", "danger")
            return redirect(url_for("atividades_notdone"))

        if dt_to < dt_from:
            flash("O campo 'Até' não pode ser menor que 'De'.", "danger")
            return redirect(url_for("atividades_notdone"))

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            if tipo == "clientes":
                cur.execute("""
                    SELECT
                        activity_id,
                        `date`,
                        city,
                        customer_number,
                        customer_phone,
                        customer_name,
                        appt_number,
                        origin_bucket,
                        ser_clo_imp_ada,
                        resource_id,
                        tratativa_status,
                        tratado_por_username,
                        tratado_em,
                        created_at
                    FROM ofs_atividades_notdone
                    WHERE `date` BETWEEN %s AND %s
                    ORDER BY `date` ASC, created_at DESC
                """, (date_from, date_to))

                rows = cur.fetchall()

                sheet_name = "Clientes"
                headers = [
                    "activity_id", "date", "city",
                    "customer_number", "customer_phone", "customer_name",
                    "appt_number", "origin_bucket",
                    "ser_clo_imp_ada", "resource_id",
                    "tratativa_status", "tratado_por_username", "tratado_em", "created_at"
                ]

            else:
                cur.execute("""
                    SELECT
                        h.id AS history_id,
                        h.activity_id,
                        n.customer_name,
                        n.customer_number,
                        n.appt_number,
                        h.action,
                        h.status,
                        h.obs,
                        h.actor_username,
                        h.created_at
                    FROM ofs_atividades_notdone_history h
                    LEFT JOIN ofs_atividades_notdone n
                      ON n.activity_id = h.activity_id
                    WHERE h.created_at BETWEEN %s AND %s
                    ORDER BY h.created_at DESC
                """, (f"{dt_from} 00:00:00", f"{dt_to} 23:59:59"))

                rows = cur.fetchall()

                sheet_name = "Tratativas"
                headers = [
                    "history_id", "activity_id",
                    "customer_name", "customer_number", "appt_number",
                    "action", "status", "obs", "actor_username", "created_at"
                ]

        finally:
            cur.close()
            conn.close()

        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name
        ws.append(headers)

        for r in rows:
            ws.append([r.get(h) for h in headers])

        xlsx_auto_width(ws)

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ofs_notdone_{tipo}_{dt_from}_{dt_to}_{stamp}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    @app.route("/atividades-notdone", methods=["GET"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone():
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = (request.args.get("dateFrom") or today).strip()
        date_to = (request.args.get("dateTo") or today).strip()
        resources = (request.args.get("resources") or "MG").strip()

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT
                activity_id AS activityId,
                city,
                customer_number AS customerNumber,
                customer_name AS customerName,
                appt_number AS apptNumber,
                origin_bucket AS XA_ORIGIN_BUCKET,
                tsk_not AS XA_TSK_NOT,
                ser_clo_imp_ada AS XA_SER_CLO_IMP_ADA,
                resource_id AS resourceId,
                date AS date,
                tratativa_status,
                tratativa_obs,
                tratado_por_username,
                tratado_em
            FROM ofs_atividades_notdone
            ORDER BY
                (tratado_em IS NULL) DESC,
                created_at DESC
            LIMIT 5000
        """)
        items = cur.fetchall()
        cur.close()
        conn.close()

        total = len(items)
        tratados = sum(1 for i in items if i.get("tratado_em"))
        pendentes = total - tratados

        return render_template(
            "atividades_notdone.html",
            items=items,
            date_from=date_from,
            date_to=date_to,
            resources=resources,
            total=total,
            tratados=tratados,
            pendentes=pendentes,
        )

    @app.route("/atividades-notdone/importar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_importar():
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = (request.form.get("dateFrom") or today).strip()
        date_to = (request.form.get("dateTo") or today).strip()
        resources = (request.form.get("resources") or "MG").strip()

        client = OFSClient()

        fields = [
            "activityId",
            "city",
            "customerNumber",
            "customerName",
            "customerPhone",
            "apptNumber",
            "XA_ORIGIN_BUCKET",
            "XA_TSK_NOT",
            "XA_SER_CLO_IMP_ADA",
            "resourceId",
            "date",
        ]

        base_params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "resources": resources,
            "q": "status=='notdone'",
            "fields": ",".join(fields),
            "limit": 2000,
            "offset": 0,
        }

        items = []
        has_more = True
        max_pages = 20
        page = 0

        try:
            while has_more and page < max_pages:
                qs = urlencode(base_params, safe="=,'")
                url = f"{client.base_url}/activities/?{qs}"
                data = client.authenticated_get(url)

                batch = data.get("items") or []
                items.extend(batch)

                has_more = bool(data.get("hasMore"))
                if has_more:
                    base_params["offset"] = len(items)
                page += 1
        except Exception as e:
            flash(f"❌ Falha ao importar da API: {e}", "danger")
            return redirect(url_for("atividades_notdone", dateFrom=date_from, dateTo=date_to, resources=resources))

        conn = get_connection()
        cur = conn.cursor()

        inserted = 0
        skipped = 0

        sql = """
            INSERT IGNORE INTO ofs_atividades_notdone
            (activity_id, city, customer_number, customer_phone, customer_name, appt_number, origin_bucket, tsk_not, ser_clo_imp_ada, resource_id,date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        for a in items:
            activity_id = str(a.get("activityId") or "").strip()
            if not activity_id:
                continue

            cur.execute(sql, (
                activity_id,
                str(a.get("city") or "") or None,
                str(a.get("customerNumber") or "") or None,
                str(a.get("customerPhone") or "") or None,
                str(a.get("customerName") or "") or None,
                str(a.get("apptNumber") or "") or None,
                str(a.get("XA_ORIGIN_BUCKET") or "") or None,
                a.get("XA_TSK_NOT"),
                str(a.get("XA_SER_CLO_IMP_ADA") or "") or None,
                str(a.get("resourceId") or "") or None,
                str(a.get("date") or "") or None,
            ))
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cur.close()
        conn.close()

        flash(f"✅ Importação concluída. Novos: {inserted} | Já existiam: {skipped}", "success")
        return redirect(url_for("atividades_notdone", dateFrom=date_from, dateTo=date_to, resources=resources))

    @app.route("/atividades-notdone/tratar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_tratar():
        data = request.get_json(silent=True) or {}

        activity_id = str(data.get("activityId") or "").strip()
        status = (data.get("status") or "").strip()
        obs = (data.get("observacoes") or "").strip()

        allowed = {
            "Reagendado",
            "Sem contato",
            "Reagendado sem contato",
            "Visita cancelada",
            "Aberto Lecom (Crescimento Organico)"
        }

        if not activity_id:
            return jsonify({"ok": False, "error": "activityId obrigatório"}), 400
        if status not in allowed:
            return jsonify({"ok": False, "error": "status inválido"}), 400

        actor = current_actor()
        user_id = actor.get("id")
        username = actor.get("username")

        conn = get_connection()
        cur = conn.cursor()

        try:
            update_sql = """
                UPDATE ofs_atividades_notdone
                SET
                    tratativa_status = %s,
                    tratativa_obs = %s,
                    tratado_por_user_id = %s,
                    tratado_por_username = %s,
                    tratado_em = NOW()
                WHERE activity_id = %s
                  AND tratado_em IS NULL
            """
            cur.execute(update_sql, (
                status,
                obs if obs else None,
                int(user_id) if user_id else None,
                username,
                activity_id
            ))

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({
                    "ok": False,
                    "error": "Esta atividade já foi tratada por outro usuário (ou não existe no banco)."
                }), 409

            cur.execute("""
                INSERT INTO ofs_atividades_notdone_history
                (activity_id, action, status, obs, actor_user_id, actor_username)
                VALUES (%s, 'TRATAR', %s, %s, %s, %s)
            """, (
                activity_id,
                status,
                obs if obs else None,
                int(user_id) if user_id else None,
                username
            ))

            conn.commit()

        except Exception as e:
            conn.rollback()
            return jsonify({"ok": False, "error": f"Erro ao salvar tratativa: {e}"}), 500

        finally:
            cur.close()
            conn.close()

        try:
            audit_log(
                actor_user_id=user_id,
                actor_username=username,
                module="ofs",
                action="tratativa_notdone",
                entity_type="activity",
                entity_ref=activity_id,
                summary=f"Tratou atividade notdone: activityId={activity_id} status={status}",
                meta={"status": status},
            )
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "activityId": activity_id,
            "tratadoPor": username,
            "status": status
        }), 200

    @app.route("/atividades-notdone/<activity_id>", methods=["GET"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_get(activity_id):
        activity_id = str(activity_id or "").strip()
        if not activity_id:
            return jsonify({"ok": False, "error": "activityId inválido"}), 400

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT
                activity_id AS activityId,
                city,
                customer_number AS customerNumber,
                customer_phone AS customerPhone,
                customer_name AS customerName,
                appt_number AS apptNumber,
                origin_bucket AS XA_ORIGIN_BUCKET,
                tsk_not AS XA_TSK_NOT,
                ser_clo_imp_ada AS XA_SER_CLO_IMP_ADA,
                resource_id AS resourceId,
                date,
                tratativa_status,
                tratativa_obs,
                tratado_por_username,
                tratado_em
            FROM ofs_atividades_notdone
            WHERE activity_id = %s
            LIMIT 1
        """, (activity_id,))
        row = cur.fetchone()

        cur.close()
        conn.close()

        if not row:
            return jsonify({"ok": False, "error": "Não encontrado"}), 404

        return jsonify({"ok": True, "item": row}), 200

    @app.route("/atividades-notdone/revogar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_revogar():
        data = request.get_json(silent=True) or {}

        activity_id = str(data.get("activityId") or "").strip()
        obs = (data.get("observacoes") or "").strip()

        if not activity_id:
            return jsonify({"ok": False, "error": "activityId obrigatório"}), 400
        if len(obs) < 3:
            return jsonify({"ok": False, "error": "Observação obrigatória para revogar"}), 400

        actor = current_actor()
        user_id = actor.get("id")
        username = actor.get("username")

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT tratativa_status, tratativa_obs, tratado_em, tratado_por_username
                FROM ofs_atividades_notdone
                WHERE activity_id = %s
                LIMIT 1
            """, (activity_id,))
            row = cur.fetchone()

            if not row:
                conn.rollback()
                return jsonify({"ok": False, "error": "activityId não encontrado no banco"}), 404

            if row.get("tratado_em") is None:
                conn.rollback()
                return jsonify({"ok": False, "error": "Este caso não está tratado"}), 409

            before_status = row.get("tratativa_status")
            before_obs = row.get("tratativa_obs")
            before_user = row.get("tratado_por_username")
            before_dt = row.get("tratado_em")

            cur.execute("""
                INSERT INTO ofs_atividades_notdone_history
                (activity_id, action, status, obs, actor_user_id, actor_username)
                VALUES (%s, 'REVOGAR', %s, %s, %s, %s)
            """, (
                activity_id,
                before_status,
                (
                    f"[REVOGAÇÃO] {obs}\n\n"
                    f"[ANTES] status={before_status or '-'} | por={before_user or '-'} | em={before_dt or '-'}\n"
                    f"[OBS ANTERIOR] {before_obs or '-'}"
                ),
                int(user_id) if user_id else None,
                username
            ))

            cur.execute("""
                UPDATE ofs_atividades_notdone
                SET
                    tratativa_status = NULL,
                    tratativa_obs = NULL,
                    tratado_por_user_id = NULL,
                    tratado_por_username = NULL,
                    tratado_em = NULL
                WHERE activity_id = %s
                  AND tratado_em IS NOT NULL
            """, (activity_id,))

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"ok": False, "error": "Este caso já foi revogado por outro usuário."}), 409

            conn.commit()

        except Exception as e:
            conn.rollback()
            return jsonify({"ok": False, "error": f"Erro ao revogar: {e}"}), 500

        finally:
            cur.close()
            conn.close()

        try:
            audit_log(
                actor_user_id=user_id,
                actor_username=username,
                module="ofs",
                action="revogar_tratativa_notdone",
                entity_type="activity",
                entity_ref=activity_id,
                summary=f"Revogou tratativa notdone: activityId={activity_id}",
                meta={"obs": obs[:500]},
            )
        except Exception:
            pass

        return jsonify({"ok": True, "activityId": activity_id}), 200