from __future__ import annotations

from datetime import date

from flask import current_app, jsonify, request

from core.auth import has_perm, login_required
from services.ofs_technician_monitor_service import (
    TechnicianMonitorService,
    default_monitor_work_date,
)


PERMISSION = "dashboard.operacional_acessar"


def _service() -> TechnicianMonitorService:
    return TechnicianMonitorService()


def _work_date_arg() -> date:
    raw = (request.args.get("date") or "").strip()
    if not raw:
        return default_monitor_work_date()
    return date.fromisoformat(raw)


def _bool_arg(name: str) -> bool:
    return (request.args.get(name) or "").strip().lower() in {"1", "true", "yes", "sim", "on"}


def _denied():
    return jsonify({
        "ok": False,
        "error": {
            "code": "FORBIDDEN",
            "message": "Acesso negado para este recurso.",
        },
    }), 403


def _internal_error():
    return jsonify({
        "ok": False,
        "error": {
            "code": "READ_MODEL_ERROR",
            "message": "Não foi possível consultar o monitor operacional local.",
        },
    }), 500


def init_app(app):
    @app.route("/dashboard/technicians/summary")
    @login_required
    def technician_monitor_summary():
        if not has_perm(PERMISSION):
            return _denied()

        try:
            work_date = _work_date_arg()
        except ValueError:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "INVALID_DATE",
                    "message": "Parâmetro date deve usar o formato YYYY-MM-DD.",
                },
            }), 400

        try:
            payload, _metrics = _service().build_summary(work_date)
            return jsonify({"ok": True, "data": payload}), 200
        except Exception:
            current_app.logger.exception("Falha ao consultar summary local de técnicos.")
            return _internal_error()

    @app.route("/dashboard/technicians/tree")
    @login_required
    def technician_monitor_tree():
        if not has_perm(PERMISSION):
            return _denied()

        try:
            work_date = _work_date_arg()
        except ValueError:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "INVALID_DATE",
                    "message": "Parâmetro date deve usar o formato YYYY-MM-DD.",
                },
            }), 400

        # Demanda 08: medição real decidiu por lazy loading incremental.
        # full permanece explícito apenas para diagnóstico/benchmark.
        mode = (request.args.get("mode") or "children").strip().lower()
        parent_id = (request.args.get("parent_id") or "").strip() or None
        if parent_id is not None:
            mode = "children"
        if mode not in {"full", "children"}:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "INVALID_TREE_MODE",
                    "message": "Parâmetro mode deve ser full ou children.",
                },
            }), 400

        try:
            payload, _metrics = _service().build_tree(
                work_date,
                mode=mode,
                parent_id=parent_id,
                only_problems=_bool_arg("only_problems"),
                detail=_bool_arg("detail"),
            )
            return jsonify({"ok": True, "data": payload}), 200
        except Exception:
            current_app.logger.exception("Falha ao consultar árvore local de técnicos.")
            return _internal_error()
