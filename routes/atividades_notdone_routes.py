from math import ceil
from urllib.parse import urlencode
from datetime import datetime
from io import BytesIO

from flask import render_template, request, send_file, redirect, url_for, flash, jsonify
from openpyxl import Workbook

from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from core.auth import login_required, perm_required, current_actor
from core.utils import xlsx_auto_width


PER_PAGE = 300

TIPOS_DESCONSIDERADOS = {
    "RET",
    "APR",
    "LUNCH",
    "MAN",
    "PRE",
    "ALM",
    "MAN_VEIC",
    "REUNIAO",
    "EQP_DUP",
}


def init_app(app):

    def _parse_period():
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = (request.args.get("dateFrom") or today).strip()
        date_to = (request.args.get("dateTo") or today).strip()
        resources = (request.args.get("resources") or "MG").strip()

        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        except Exception:
            flash("Período inválido. Foi aplicado o dia atual automaticamente.", "danger")
            date_from = today
            date_to = today
            dt_from = datetime.strptime(today, "%Y-%m-%d").date()
            dt_to = dt_from

        if dt_to < dt_from:
            flash("O campo 'Até' não pode ser menor que 'De'. Foi aplicado o dia atual.", "danger")
            date_from = today
            date_to = today
            dt_from = datetime.strptime(today, "%Y-%m-%d").date()
            dt_to = dt_from

        page_raw = (request.args.get("page") or "1").strip()
        try:
            page = int(page_raw)
        except ValueError:
            page = 1
        if page < 1:
            page = 1

        return date_from, date_to, resources, page

    def _get_kpis(date_from, date_to):
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN tratado_em IS NULL THEN 1 ELSE 0 END) AS pendentes,
                    SUM(CASE WHEN tratado_em IS NOT NULL THEN 1 ELSE 0 END) AS tratados
                FROM ofs_atividades_notdone
                WHERE `date` BETWEEN %s AND %s
            """, (date_from, date_to))
            row = cur.fetchone() or {}
            return {
                "total": int(row.get("total") or 0),
                "pendentes": int(row.get("pendentes") or 0),
                "tratados": int(row.get("tratados") or 0),
            }
        finally:
            cur.close()
            conn.close()

    def _get_items(date_from, date_to, page, view_mode):
        where_status = "tratado_em IS NULL" if view_mode == "pendentes" else "tratado_em IS NOT NULL"
        offset = (page - 1) * PER_PAGE

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute(f"""
                SELECT COUNT(*) AS total_items
                FROM ofs_atividades_notdone
                WHERE `date` BETWEEN %s AND %s
                  AND {where_status}
            """, (date_from, date_to))
            total_items = int((cur.fetchone() or {}).get("total_items") or 0)

            total_pages = max(1, ceil(total_items / PER_PAGE)) if total_items else 1
            if page > total_pages:
                page = total_pages
                offset = (page - 1) * PER_PAGE

            order_sql = "tratado_em DESC, created_at DESC" if view_mode == "tratadas" else "created_at DESC"

            cur.execute(f"""
                SELECT
                    activity_id AS activityId,
                    activity_type AS activityType,
                    city,
                    customer_number AS customerNumber,
                    customer_name AS customerName,
                    appt_number AS apptNumber,
                    origin_bucket AS XA_ORIGIN_BUCKET,
                    tsk_not AS XA_TSK_NOT,
                    ser_clo_imp_ada AS XA_SER_CLO_IMP_ADA,
                    resource_id AS resourceId,
                    `date` AS date,
                    tratativa_status,
                    tratativa_obs,
                    tratado_por_username,
                    tratado_em
                FROM ofs_atividades_notdone
                WHERE `date` BETWEEN %s AND %s
                  AND {where_status}
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, (date_from, date_to, PER_PAGE, offset))
            items = cur.fetchall()

            return items, total_items, total_pages, page
        finally:
            cur.close()
            conn.close()

    def _build_page_url(endpoint_name, page, date_from, date_to, resources):
        return url_for(
            endpoint_name,
            page=page,
            dateFrom=date_from,
            dateTo=date_to,
            resources=resources,
        )

    def _render_atividades_notdone(view_mode):
        date_from, date_to, resources, page = _parse_period()
        kpis = _get_kpis(date_from, date_to)
        items, total_items, total_pages, current_page = _get_items(date_from, date_to, page, view_mode)

        endpoint_name = "atividades_notdone" if view_mode == "pendentes" else "atividades_notdone_tratadas"

        prev_page_url = None
        next_page_url = None

        if current_page > 1:
            prev_page_url = _build_page_url(endpoint_name, current_page - 1, date_from, date_to, resources)

        if current_page < total_pages:
            next_page_url = _build_page_url(endpoint_name, current_page + 1, date_from, date_to, resources)

        return render_template(
            "atividades_notdone.html",
            items=items,
            date_from=date_from,
            date_to=date_to,
            resources=resources,
            total=kpis["total"],
            tratados=kpis["tratados"],
            pendentes=kpis["pendentes"],
            total_items=total_items,
            per_page=PER_PAGE,
            page=current_page,
            total_pages=total_pages,
            prev_page_url=prev_page_url,
            next_page_url=next_page_url,
            view_mode=view_mode,
        )

    @app.route("/atividades-notdone/exportar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_exportar():
        tipo = (request.form.get("tipo") or "").strip().lower()
        date_from = (request.form.get("dateFrom") or "").strip()
        date_to = (request.form.get("dateTo") or "").strip()
        current_view = (request.form.get("currentView") or "pendentes").strip().lower()

        redirect_endpoint = "atividades_notdone_tratadas" if current_view == "tratadas" else "atividades_notdone"

        if tipo not in {"clientes", "tratativas"}:
            flash("Tipo de exportação inválido.", "danger")
            return redirect(url_for(redirect_endpoint))

        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        except Exception:
            flash("Informe um período válido (De / Até).", "danger")
            return redirect(url_for(redirect_endpoint))

        if dt_to < dt_from:
            flash("O campo 'Até' não pode ser menor que 'De'.", "danger")
            return redirect(url_for(redirect_endpoint))

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            if tipo == "clientes":
                cur.execute("""
                    SELECT
                        activity_id,
                        activity_type,
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
                    "activity_id", "activity_type", "date", "city",
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
                        n.activity_type,
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
                    "history_id", "activity_id", "activity_type",
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

        try:
            actor = current_actor()
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action=f"export_notdone_{tipo}_xlsx",
                entity_type="atividades_notdone",
                entity_ref=tipo,
                summary=f"Exportou XLSX atividades notdone: tipo={tipo}",
                meta={
                    "tipo": tipo,
                    "date_from": str(dt_from),
                    "date_to": str(dt_to),
                    "current_view": current_view,
                    "total_rows": len(rows),
                    "filename": filename,
                },
            )
        except Exception:
            pass

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
        return _render_atividades_notdone("pendentes")

    @app.route("/atividades-notdone/tratadas", methods=["GET"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_tratadas():
        return _render_atividades_notdone("tratadas")

    @app.route("/atividades-notdone/importar", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_importar():
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = (request.form.get("dateFrom") or today).strip()
        date_to = (request.form.get("dateTo") or today).strip()
        resources = (request.form.get("resources") or "MG").strip()
        current_view = (request.form.get("currentView") or "pendentes").strip().lower()

        redirect_endpoint = "atividades_notdone_tratadas" if current_view == "tratadas" else "atividades_notdone"

        client = OFSClient()

        fields = [
            "activityId",
            "activityType",
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
            return redirect(url_for(redirect_endpoint, dateFrom=date_from, dateTo=date_to, resources=resources))

        conn = get_connection()
        cur = conn.cursor()

        inserted = 0
        skipped = 0
        ignored_types = 0

        sql = """
            INSERT IGNORE INTO ofs_atividades_notdone
            (
                activity_id,
                activity_type,
                city,
                customer_number,
                customer_phone,
                customer_name,
                appt_number,
                origin_bucket,
                tsk_not,
                ser_clo_imp_ada,
                resource_id,
                date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        for a in items:
            activity_id = str(a.get("activityId") or "").strip()
            activity_type = str(a.get("activityType") or "").strip().upper()

            if not activity_id:
                continue

            if activity_type in TIPOS_DESCONSIDERADOS:
                ignored_types += 1
                continue

            cur.execute(sql, (
                activity_id,
                activity_type or None,
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

        flash(
            f"✅ Importação concluída. Novos: {inserted} | Já existiam: {skipped} | Desconsiderados por tipo: {ignored_types}",
            "success"
        )
        return redirect(url_for(redirect_endpoint, dateFrom=date_from, dateTo=date_to, resources=resources))

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
            "status": status,
            "tratadoEm": datetime.now().isoformat(),
            "observacoes": obs,
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
                activity_type AS activityType,
                city,
                customer_number AS customerNumber,
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


    @app.route("/atividades-notdone/<activity_id>/telefone", methods=["POST"])
    @login_required
    @perm_required("ofs.atividades_notdone")
    def atividades_notdone_phone(activity_id):
        activity_id = str(activity_id or "").strip()

        if not activity_id:
            return jsonify({"ok": False, "error": "activityId invalido"}), 400

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    activity_id,
                    customer_phone,
                    customer_name,
                    customer_number,
                    appt_number
                FROM ofs_atividades_notdone
                WHERE activity_id = %s
                LIMIT 1
            """, (activity_id,))
            row = cur.fetchone()

        finally:
            cur.close()
            conn.close()

        if not row:
            return jsonify({"ok": False, "error": "Atividade nao encontrada"}), 404

        phone = str(row.get("customer_phone") or "").strip()
        customer_name = str(row.get("customer_name") or "").strip()
        customer_number = str(row.get("customer_number") or "").strip()
        appt_number = str(row.get("appt_number") or "").strip()

        last4 = phone[-4:] if phone else ""
        phone_masked = ("*" * max(0, len(phone) - 4) + last4) if phone else ""

        actor = current_actor()

        try:
            audit_log(
                actor_user_id=actor.get("id"),
                actor_username=actor.get("username"),
                module="ofs",
                action="view_notdone_customer_phone",
                entity_type="activity",
                entity_ref=activity_id,
                summary=f"Visualizou telefone de cliente em atividade notdone: activityId={activity_id}",
                meta={
                    "activity_id": activity_id,
                    "customer_name": customer_name,
                    "customer_number": customer_number,
                    "appt_number": appt_number,
                    "phone_last4": last4,
                    "phone_masked": phone_masked,
                },
            )
        except Exception:
            pass

        if not phone:
            return jsonify({
                "ok": False,
                "error": "Telefone nao encontrado para esta atividade."
            }), 404

        return jsonify({
            "ok": True,
            "activityId": activity_id,
            "phone": phone,
        }), 200


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