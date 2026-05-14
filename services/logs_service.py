from datetime import datetime, timedelta
from io import BytesIO, StringIO
from math import ceil
import csv
import json

from openpyxl import Workbook

from database.connection import get_connection
from core.utils import xlsx_auto_width


LOGS_PER_PAGE_OPTIONS = [25, 50, 100, 200]
DEFAULT_LOGS_PER_PAGE = 50
MAX_EXPORT_ROWS = 200000
EXPORT_MAX_DAYS = 90


def validate_export_period(filters, max_days=EXPORT_MAX_DAYS):
    date_ini = _safe_date(filters.get("date_ini"))
    date_fim = _safe_date(filters.get("date_fim"))

    if not date_ini or not date_fim:
        return False, f"Informe data inicial e data final para exportar. Limite maximo: {max_days} dias."

    try:
        dt_ini = datetime.strptime(date_ini, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(date_fim, "%Y-%m-%d").date()
    except Exception:
        return False, "Periodo invalido para exportacao."

    if dt_fim < dt_ini:
        return False, "A data final nao pode ser menor que a data inicial."

    total_days = (dt_fim - dt_ini).days + 1

    if total_days > max_days:
        return False, f"Periodo muito grande para exportacao. Limite maximo: {max_days} dias."

    return True, ""


def _safe_date(value):
    value = str(value or "").strip()
    if not value:
        return ""

    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except Exception:
        return ""


def _safe_int(value, default, minimum=None, maximum=None):
    try:
        value = int(value)
    except Exception:
        value = default

    if minimum is not None and value < minimum:
        value = minimum

    if maximum is not None and value > maximum:
        value = maximum

    return value


def build_log_filters(args, dashboard=False):
    today = datetime.now().date()
    default_ini = ""
    default_fim = ""

    if dashboard:
        default_ini = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        default_fim = today.strftime("%Y-%m-%d")

    date_ini = _safe_date(args.get("date_ini")) or default_ini
    date_fim = _safe_date(args.get("date_fim")) or default_fim

    return {
        "user": str(args.get("user") or "").strip(),
        "module": str(args.get("module") or "").strip(),
        "action": str(args.get("action") or "").strip(),
        "q": str(args.get("q") or "").strip(),
        "date_ini": date_ini,
        "date_fim": date_fim,
    }


def _audit_where(filters):
    where = ["1=1"]
    params = []

    user = filters.get("user")
    module = filters.get("module")
    action = filters.get("action")
    q = filters.get("q")
    date_ini = filters.get("date_ini")
    date_fim = filters.get("date_fim")

    if user:
        where.append("actor_username LIKE %s")
        params.append(f"%{user}%")

    if module:
        where.append("module = %s")
        params.append(module)

    if action:
        where.append("action = %s")
        params.append(action)

    if q:
        like = f"%{q}%"
        where.append("""
            (
                summary LIKE %s
                OR entity_ref LIKE %s
                OR entity_type LIKE %s
                OR actor_username LIKE %s
                OR module LIKE %s
                OR action LIKE %s
                OR meta_json LIKE %s
                OR CAST(api_response AS CHAR) LIKE %s
            )
        """)
        params.extend([like, like, like, like, like, like, like, like])

    if date_ini:
        where.append("created_at >= %s")
        params.append(f"{date_ini} 00:00:00")

    if date_fim:
        where.append("created_at <= %s")
        params.append(f"{date_fim} 23:59:59")

    return " AND ".join(where), params


def _history_where(filters):
    where = ["1=1"]
    params = []

    user = filters.get("user")
    q = filters.get("q")
    date_ini = filters.get("date_ini")
    date_fim = filters.get("date_fim")

    if user:
        where.append("actor_username LIKE %s")
        params.append(f"%{user}%")

    if q:
        like = f"%{q}%"
        where.append("""
            (
                activity_id LIKE %s
                OR action LIKE %s
                OR status LIKE %s
                OR obs LIKE %s
                OR actor_username LIKE %s
            )
        """)
        params.extend([like, like, like, like, like])

    if date_ini:
        where.append("created_at >= %s")
        params.append(f"{date_ini} 00:00:00")

    if date_fim:
        where.append("created_at <= %s")
        params.append(f"{date_fim} 23:59:59")

    return " AND ".join(where), params


def _json_text(raw, max_len=10000):
    if raw is None:
        return ""

    if isinstance(raw, (dict, list)):
        text = json.dumps(raw, ensure_ascii=False)
    else:
        text = str(raw)

    return text[:max_len]


def _parse_json(raw):
    if not raw:
        return {}

    if isinstance(raw, dict):
        return raw

    try:
        return json.loads(str(raw))
    except Exception:
        return {}


def _dt_text(value):
    if not value:
        return ""

    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    return str(value)


def _date_text(value):
    if not value:
        return ""

    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")

    return str(value)


def _add_percent(rows, total_key="total"):
    max_value = 0

    for row in rows:
        try:
            max_value = max(max_value, int(row.get(total_key) or 0))
        except Exception:
            pass

    for row in rows:
        try:
            value = int(row.get(total_key) or 0)
        except Exception:
            value = 0

        row["percent"] = int((value / max_value) * 100) if max_value else 0

    return rows


def list_filter_options():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT DISTINCT module
            FROM audit_log
            WHERE module IS NOT NULL AND module <> ''
            ORDER BY module
        """)
        modules = [r["module"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT action
            FROM audit_log
            WHERE action IS NOT NULL AND action <> ''
            ORDER BY action
        """)
        actions = [r["action"] for r in cur.fetchall()]

        return modules, actions

    finally:
        cur.close()
        conn.close()


def fetch_audit_logs(filters, page=1, per_page=DEFAULT_LOGS_PER_PAGE):
    page = _safe_int(page, 1, minimum=1)
    per_page = _safe_int(per_page, DEFAULT_LOGS_PER_PAGE, minimum=25, maximum=200)

    if per_page not in LOGS_PER_PAGE_OPTIONS:
        per_page = DEFAULT_LOGS_PER_PAGE

    where_sql, params = _audit_where(filters)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM audit_log
            WHERE {where_sql}
            """,
            tuple(params),
        )
        total = int((cur.fetchone() or {}).get("total") or 0)

        total_pages = max(1, ceil(total / per_page)) if total else 1

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * per_page

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
            FROM audit_log
            WHERE {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [per_page, offset]),
        )

        logs = cur.fetchall()

        for row in logs:
            row["api_response_text"] = _json_text(row.get("api_response"))

        page_start = offset + 1 if total else 0
        page_end = min(offset + per_page, total) if total else 0

        return {
            "logs": logs,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "page_start": page_start,
            "page_end": page_end,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        }

    finally:
        cur.close()
        conn.close()


def export_audit_logs_csv(filters):
    where_sql, params = _audit_where(filters)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            f"""
            SELECT
                created_at,
                actor_username,
                module,
                action,
                summary,
                entity_type,
                entity_ref,
                meta_json
            FROM audit_log
            WHERE {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT {MAX_EXPORT_ROWS}
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    finally:
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
        "meta_json",
    ])

    for row in rows:
        writer.writerow([
            _dt_text(row.get("created_at")),
            row.get("actor_username") or "",
            row.get("module") or "",
            row.get("action") or "",
            row.get("summary") or "",
            row.get("entity_type") or "",
            row.get("entity_ref") or "",
            row.get("meta_json") or "",
        ])

    output = BytesIO(text_buffer.getvalue().encode("utf-8-sig"))
    filename = f"audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return output, filename, len(rows)


def _fetch_all(cur, sql, params=None):
    cur.execute(sql, tuple(params or []))
    return cur.fetchall()


def _fetch_one(cur, sql, params=None):
    cur.execute(sql, tuple(params or []))
    return cur.fetchone() or {}


def _download_condition():
    return """
        (
            LOCATE('download', action) > 0
            OR LOCATE('export', action) > 0
            OR LOCATE('Baixou', summary) > 0
            OR LOCATE('Export', summary) > 0
            OR LOCATE('CSV', summary) > 0
            OR LOCATE('XLSX', summary) > 0
            OR LOCATE('Excel', summary) > 0
        )
    """


def _critical_condition():
    return """
        (
            (module = 'auth' AND action = 'login_failed')
            OR (module = 'perfis' AND action IN ('create', 'update', 'delete'))
            OR (module = 'usuarios' AND action IN ('create', 'admin_reset_password', 'change_password'))
            OR (module = 'relatorios' AND (
                LOCATE('download', action) > 0
                OR LOCATE('report', action) > 0
                OR LOCATE('sync', action) > 0
            ))
            OR (module = 'ofs' AND action IN ('tratativa_notdone', 'revogar_tratativa_notdone', 'view_notdone_customer_phone', 'cleanup', 'cleanup_simulation', 'bulk_update_user_type', 'update_user_type', 'create_tecnicos_csv'))
            OR (module = 'adapter' AND action = 'close_os')
        )
    """


def get_logs_dashboard_data(filters):
    audit_where, audit_params = _audit_where(filters)
    history_where, history_params = _history_where(filters)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        audit_kpis = _fetch_one(
            cur,
            f"""
            SELECT
                COUNT(*) AS total_logs,
                COUNT(DISTINCT actor_username) AS usuarios_ativos,
                COUNT(DISTINCT module) AS modulos_utilizados,
                COUNT(DISTINCT action) AS acoes_registradas
            FROM audit_log
            WHERE {audit_where}
            """,
            audit_params,
        )

        downloads = _fetch_one(
            cur,
            f"""
            SELECT COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND {_download_condition()}
            """,
            audit_params,
        )

        report_downloads = _fetch_one(
            cur,
            f"""
            SELECT COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND action = 'download_ofs_os_report'
            """,
            audit_params,
        )

        login_failed = _fetch_one(
            cur,
            f"""
            SELECT COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND module = 'auth'
              AND action = 'login_failed'
            """,
            audit_params,
        )

        close_os = _fetch_one(
            cur,
            f"""
            SELECT COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND module = 'adapter'
              AND action = 'close_os'
            """,
            audit_params,
        )

        history_kpis = _fetch_one(
            cur,
            f"""
            SELECT
                SUM(CASE WHEN action = 'TRATAR' THEN 1 ELSE 0 END) AS tratativas,
                SUM(CASE WHEN action = 'REVOGAR' THEN 1 ELSE 0 END) AS revogacoes,
                COUNT(*) AS total_historico
            FROM ofs_atividades_notdone_history
            WHERE {history_where}
            """,
            history_params,
        )

        top_modules = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT module, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
            GROUP BY module
            ORDER BY total DESC
            LIMIT 15
            """,
            audit_params,
        ))

        top_actions = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT module, action, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
            GROUP BY module, action
            ORDER BY total DESC
            LIMIT 20
            """,
            audit_params,
        ))

        top_users = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT actor_username, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
            GROUP BY actor_username
            ORDER BY total DESC
            LIMIT 20
            """,
            audit_params,
        ))

        downloads_by_user = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT actor_username, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND {_download_condition()}
            GROUP BY actor_username
            ORDER BY total DESC
            LIMIT 20
            """,
            audit_params,
        ))

        downloads_detail = _fetch_all(
            cur,
            f"""
            SELECT
                id,
                created_at,
                actor_username,
                action,
                entity_ref,
                summary,
                meta_json
            FROM audit_log
            WHERE {audit_where}
              AND {_download_condition()}
            ORDER BY created_at DESC, id DESC
            LIMIT 50
            """,
            audit_params,
        )

        for row in downloads_detail:
            meta = _parse_json(row.get("meta_json"))
            row["filename"] = meta.get("filename") or ""
            row["dateFrom"] = meta.get("dateFrom") or ""
            row["dateTo"] = meta.get("dateTo") or ""
            row["total_rows"] = meta.get("total_rows") or ""
            row["created_at_text"] = _dt_text(row.get("created_at"))

        notdone_by_user = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT
                actor_username,
                SUM(CASE WHEN action = 'TRATAR' THEN 1 ELSE 0 END) AS tratativas,
                SUM(CASE WHEN action = 'REVOGAR' THEN 1 ELSE 0 END) AS revogacoes,
                COUNT(*) AS total
            FROM ofs_atividades_notdone_history
            WHERE {history_where}
            GROUP BY actor_username
            ORDER BY total DESC
            LIMIT 20
            """,
            history_params,
        ))

        notdone_by_status = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT
                action,
                status,
                COUNT(*) AS total
            FROM ofs_atividades_notdone_history
            WHERE {history_where}
            GROUP BY action, status
            ORDER BY total DESC
            LIMIT 20
            """,
            history_params,
        ))

        activity_by_day = _fetch_all(
            cur,
            f"""
            SELECT DATE(created_at) AS dia, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
            GROUP BY DATE(created_at)
            ORDER BY dia DESC
            LIMIT 31
            """,
            audit_params,
        )

        for row in activity_by_day:
            row["dia"] = _date_text(row.get("dia"))

        activity_by_hour = _fetch_all(
            cur,
            f"""
            SELECT
                LPAD(hour_value, 2, '0') AS hora,
                total
            FROM (
                SELECT
                    HOUR(created_at) AS hour_value,
                    COUNT(*) AS total
                FROM audit_log
                WHERE {audit_where}
                GROUP BY HOUR(created_at)
            ) AS grouped_hours
            ORDER BY hour_value ASC
            """,
            audit_params,
        )

        login_failed_by_user = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT actor_username, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND module = 'auth'
              AND action = 'login_failed'
            GROUP BY actor_username
            ORDER BY total DESC
            LIMIT 20
            """,
            audit_params,
        ))

        critical_by_user = _add_percent(_fetch_all(
            cur,
            f"""
            SELECT actor_username, module, action, COUNT(*) AS total
            FROM audit_log
            WHERE {audit_where}
              AND {_critical_condition()}
            GROUP BY actor_username, module, action
            ORDER BY total DESC
            LIMIT 30
            """,
            audit_params,
        ))

        recent_critical = _fetch_all(
            cur,
            f"""
            SELECT
                created_at,
                actor_username,
                module,
                action,
                summary,
                entity_ref
            FROM audit_log
            WHERE {audit_where}
              AND {_critical_condition()}
            ORDER BY created_at DESC, id DESC
            LIMIT 50
            """,
            audit_params,
        )

        for row in recent_critical:
            row["created_at_text"] = _dt_text(row.get("created_at"))

    finally:
        cur.close()
        conn.close()

    return {
        "kpis": {
            "total_logs": int(audit_kpis.get("total_logs") or 0),
            "usuarios_ativos": int(audit_kpis.get("usuarios_ativos") or 0),
            "modulos_utilizados": int(audit_kpis.get("modulos_utilizados") or 0),
            "acoes_registradas": int(audit_kpis.get("acoes_registradas") or 0),
            "downloads": int(downloads.get("total") or 0),
            "downloads_relatorio_ofs": int(report_downloads.get("total") or 0),
            "tratativas": int(history_kpis.get("tratativas") or 0),
            "revogacoes": int(history_kpis.get("revogacoes") or 0),
            "historico_notdone": int(history_kpis.get("total_historico") or 0),
            "login_failed": int(login_failed.get("total") or 0),
            "close_os_adapter": int(close_os.get("total") or 0),
        },
        "top_modules": top_modules,
        "top_actions": top_actions,
        "top_users": top_users,
        "downloads_by_user": downloads_by_user,
        "downloads_detail": downloads_detail,
        "notdone_by_user": notdone_by_user,
        "notdone_by_status": notdone_by_status,
        "activity_by_day": activity_by_day,
        "activity_by_hour": activity_by_hour,
        "login_failed_by_user": login_failed_by_user,
        "critical_by_user": critical_by_user,
        "recent_critical": recent_critical,
    }


def _append_sheet(wb, title, headers, rows):
    ws = wb.create_sheet(title=title[:31])
    ws.append(headers)

    for row in rows:
        ws.append([row.get(h) for h in headers])

    xlsx_auto_width(ws)


def export_logs_dashboard_xlsx(filters):
    data = get_logs_dashboard_data(filters)

    wb = Workbook()
    ws = wb.active
    ws.title = "Resumo"

    ws.append(["Metrica", "Valor"])

    labels = {
        "total_logs": "Total de logs",
        "usuarios_ativos": "Usuarios ativos",
        "modulos_utilizados": "Modulos utilizados",
        "acoes_registradas": "Acoes registradas",
        "downloads": "Downloads/exportacoes",
        "downloads_relatorio_ofs": "Downloads de relatorio OFS",
        "tratativas": "Tratativas notdone",
        "revogacoes": "Revogacoes notdone",
        "historico_notdone": "Historico notdone",
        "login_failed": "Falhas de login",
        "close_os_adapter": "Fechamentos via Adapter",
    }

    for key, label in labels.items():
        ws.append([label, data["kpis"].get(key, 0)])

    ws.append([])
    ws.append(["Filtro", "Valor"])
    for key in ["date_ini", "date_fim", "user", "module", "action", "q"]:
        ws.append([key, filters.get(key) or ""])

    xlsx_auto_width(ws)

    _append_sheet(wb, "Modulos", ["module", "total"], data["top_modules"])
    _append_sheet(wb, "Acoes", ["module", "action", "total"], data["top_actions"])
    _append_sheet(wb, "Usuarios", ["actor_username", "total"], data["top_users"])
    _append_sheet(wb, "Downloads por usuario", ["actor_username", "total"], data["downloads_by_user"])
    _append_sheet(
        wb,
        "Downloads detalhe",
        ["created_at_text", "actor_username", "action", "entity_ref", "filename", "dateFrom", "dateTo", "total_rows", "summary"],
        data["downloads_detail"],
    )
    _append_sheet(wb, "Tratativas por usuario", ["actor_username", "tratativas", "revogacoes", "total"], data["notdone_by_user"])
    _append_sheet(wb, "Tratativas por status", ["action", "status", "total"], data["notdone_by_status"])
    _append_sheet(wb, "Logs por dia", ["dia", "total"], data["activity_by_day"])
    _append_sheet(wb, "Logs por hora", ["hora", "total"], data["activity_by_hour"])
    _append_sheet(wb, "Falhas login", ["actor_username", "total"], data["login_failed_by_user"])
    _append_sheet(wb, "Acoes criticas", ["actor_username", "module", "action", "total"], data["critical_by_user"])
    _append_sheet(wb, "Criticos recentes", ["created_at_text", "actor_username", "module", "action", "summary", "entity_ref"], data["recent_critical"])

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"dashboard_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    return output, filename
