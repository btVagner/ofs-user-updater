from flask import render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime
from urllib.parse import urlencode

from database.connection import get_connection
from ofs.client import OFSClient
from core.auth import login_required, perm_required


def init_app(app):

    @app.route("/sap/acompanhamento-critica", methods=["GET"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_acompanhamento_critica():
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
                activity_type AS activityType,
                appt_number AS apptNumber,
                origin_bucket AS XA_ORIGIN_BUCKET,
                resource_id AS resourceId,
                xa_sap_crt AS XA_SAP_CRT,
                `date` AS date,
                created_at
            FROM sap_criticas_atividades
            WHERE `date` BETWEEN %s AND %s
            ORDER BY `date` ASC, created_at DESC
            LIMIT 5000
        """, (date_from, date_to))
        items = cur.fetchall()
        total = len(items)
        cur.close()
        conn.close()

        return render_template(
            "criticas_sap/sap_acompanhamento_critica.html",
            items=items,
            date_from=date_from,
            date_to=date_to,
            resources=resources,
            total=total,
        )

    @app.route("/sap/acompanhamento-critica/importar", methods=["POST"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_acompanhamento_critica_importar():
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = (request.form.get("dateFrom") or today).strip()
        date_to = (request.form.get("dateTo") or today).strip()
        resources = (request.form.get("resources") or "MG").strip()

        client = OFSClient()

        fields = [
            "activityId",
            "city",
            "activityType",
            "apptNumber",
            "XA_ORIGIN_BUCKET",
            "resourceId",
            "XA_SAP_CRT",
            "XA_SAP_CRT_LDG",
            "date",
        ]

        base_params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "resources": resources,
            "q": "XA_SAP_CRT==1",
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
            return redirect(url_for("sap_acompanhamento_critica", dateFrom=date_from, dateTo=date_to, resources=resources))

        conn = get_connection()
        cur = conn.cursor()

        inserted = 0
        skipped = 0

        sql = """
            INSERT IGNORE INTO sap_criticas_atividades
            (activity_id, city, activity_type, appt_number, origin_bucket, resource_id, xa_sap_crt, xa_sap_crt_ldg, `date`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        for a in items:
            activity_id = str(a.get("activityId") or "").strip()
            if not activity_id:
                continue

            cur.execute(sql, (
                activity_id,
                str(a.get("city") or "") or None,
                str(a.get("activityType") or "") or None,
                str(a.get("apptNumber") or "") or None,
                str(a.get("XA_ORIGIN_BUCKET") or "") or None,
                str(a.get("resourceId") or "") or None,
                1 if str(a.get("XA_SAP_CRT") or "").strip() == "1" else 0,
                a.get("XA_SAP_CRT_LDG"),
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
        return redirect(url_for("sap_acompanhamento_critica", dateFrom=date_from, dateTo=date_to, resources=resources))

    @app.route("/sap/acompanhamento-critica/<activity_id>", methods=["GET"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_acompanhamento_critica_get(activity_id):
        activity_id = str(activity_id or "").strip()
        if not activity_id:
            return jsonify({"ok": False, "error": "activityId inválido"}), 400

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT
                activity_id AS activityId,
                xa_sap_crt_ldg AS XA_SAP_CRT_LDG
            FROM sap_criticas_atividades
            WHERE activity_id = %s
            LIMIT 1
        """, (activity_id,))
        row = cur.fetchone()

        cur.close()
        conn.close()

        if not row:
            return jsonify({"ok": False, "error": "Não encontrado"}), 404

        return jsonify({"ok": True, "item": row}), 200

    @app.route("/sap/acompanhamento-critica/dashboard", methods=["GET"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_dashboard_critica():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT MIN(`date`) AS min_date, MAX(`date`) AS max_date FROM sap_criticas_atividades")
        mm = cur.fetchone() or {}
        min_date = (mm.get("min_date") or "")
        max_date = (mm.get("max_date") or "")

        cur.execute("""
        SELECT activity_type
        FROM sap_criticas_atividades
        WHERE activity_type IS NOT NULL AND activity_type <> ''
        GROUP BY activity_type
        ORDER BY activity_type
        """)
        types = [r["activity_type"] for r in cur.fetchall()]

        cur.execute("""
        SELECT DISTINCT tb.bucket
        FROM projToquio.td_bucket tb
        JOIN (
            SELECT DISTINCT city
            FROM sap_criticas_atividades
            WHERE city IS NOT NULL AND city <> ''
        ) c ON c.city = tb.nomeCidade
        ORDER BY tb.bucket
        """)
        buckets = [r["bucket"] for r in cur.fetchall()]

        cur.close()
        conn.close()

        return render_template(
            "criticas_sap/dashboard_critica.html",
            min_date=min_date,
            max_date=max_date,
            types=types,
            buckets=buckets
        )

    @app.route("/sap/acompanhamento-critica/dashboard/data", methods=["GET"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_dashboard_critica_data():
        date_from = (request.args.get("dateFrom") or "").strip()
        date_to = (request.args.get("dateTo") or "").strip()
        activity_type = (request.args.get("activityType") or "").strip()
        buckets = [b.strip() for b in request.args.getlist("buckets") if (b or "").strip()]

        if not date_from or not date_to:
            return jsonify({"ok": False, "error": "Informe dateFrom e dateTo."}), 400
        if date_to < date_from:
            return jsonify({"ok": False, "error": "dateTo não pode ser menor que dateFrom."}), 400

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            sql = """
                SELECT `date` AS d, COUNT(*) AS total
                FROM sap_criticas_atividades
                WHERE `date` BETWEEN %s AND %s
            """
            params = [date_from, date_to]

            if activity_type:
                sql += " AND activity_type = %s"
                params.append(activity_type)

            if buckets:
                placeholders = ",".join(["%s"] * len(buckets))
                sql += f" AND origin_bucket IN ({placeholders})"
                params.extend(buckets)

            sql += " GROUP BY `date` ORDER BY `date` ASC"

            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

            labels = [r["d"] for r in rows]
            values = [int(r["total"] or 0) for r in rows]

            return jsonify({"ok": True, "labels": labels, "values": values}), 200

        except Exception as e:
            return jsonify({"ok": False, "error": f"Erro no dashboard/data: {e}"}), 500

        finally:
            cur.close()
            conn.close()

    @app.route("/sap/acompanhamento-critica/dashboard/data2", methods=["GET"])
    @login_required
    @perm_required("sap.acompanhamento_critica")
    def sap_dashboard_critica_data2():
        date_from = (request.args.get("dateFrom") or "").strip()
        date_to = (request.args.get("dateTo") or "").strip()
        activity_type = (request.args.get("activityType") or "").strip()
        buckets = [b.strip() for b in request.args.getlist("buckets") if (b or "").strip()]

        if not date_from or not date_to:
            return jsonify({"ok": False, "error": "Informe dateFrom e dateTo."}), 400
        if date_to < date_from:
            return jsonify({"ok": False, "error": "dateTo não pode ser menor que dateFrom."}), 400

        produced_types = [
            "INS_DEV", "SOL_SER", "MIG_PLA", "MUD_END", "RET",
            "INS", "SUP", "SUP_REP", "SUP_QUA", "MIG_TEC", "QUA",
        ]

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        try:
            sql_c = """
                SELECT `date` AS d, COUNT(*) AS total
                FROM sap_criticas_atividades
                WHERE `date` BETWEEN %s AND %s
            """
            params_c = [date_from, date_to]

            if activity_type:
                sql_c += " AND activity_type = %s"
                params_c.append(activity_type)

            if buckets:
                ph = ",".join(["%s"] * len(buckets))
                sql_c += f" AND origin_bucket IN ({ph})"
                params_c.extend(buckets)

            sql_c += " GROUP BY `date` ORDER BY `date` ASC"

            cur.execute(sql_c, tuple(params_c))
            rows_c = cur.fetchall()

            labels = [r["d"] for r in rows_c]
            criticadas = [int(r["total"] or 0) for r in rows_c]

            if not labels:
                return jsonify({"ok": True, "labels": [], "criticadas": [], "produzidas": []}), 200

            sql_p = """
                SELECT b.`date` AS d, COUNT(*) AS total
                FROM ofs_atividades_base b
                WHERE b.`date` BETWEEN %s AND %s
                  AND b.status = 'completed'
            """
            params_p = [date_from, date_to]

            ph = ",".join(["%s"] * len(produced_types))
            sql_p += f" AND b.activity_type IN ({ph})"
            params_p.extend(produced_types)

            if activity_type:
                sql_p += " AND b.activity_type = %s"
                params_p.append(activity_type)

            if buckets:
                ph = ",".join(["%s"] * len(buckets))
                sql_p += f" AND b.origin_bucket IN ({ph})"
                params_p.extend(buckets)

            sql_p += " GROUP BY b.`date` ORDER BY b.`date` ASC"

            cur.execute(sql_p, tuple(params_p))
            rows_p = cur.fetchall()

            prod_map = {r["d"]: int(r["total"] or 0) for r in rows_p}
            produzidas = [prod_map.get(d, None) for d in labels]

            return jsonify({
                "ok": True,
                "labels": labels,
                "criticadas": criticadas,
                "produzidas": produzidas,
            }), 200

        except Exception as e:
            return jsonify({"ok": False, "error": f"Erro no dashboard/data2: {e}"}), 500
        finally:
            cur.close()
            conn.close()