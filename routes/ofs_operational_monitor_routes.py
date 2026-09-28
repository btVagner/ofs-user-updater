from __future__ import annotations

import hmac
import secrets

from flask import current_app, jsonify, render_template, request, session

from core.auth import current_actor, has_perm, login_required, perm_required
from services.ofs_operational_monitor_service import (
    PERMISSION,
    MonitorRefreshCooldown,
    MonitorRefreshInProgress,
    MonitorScopeError,
    MonitorSourceUnavailable,
    OperationalMonitorService,
)
from services.ofs_operational_monitor_treatment_service import (
    MonitorTreatmentService,
    SUPERVISE_PERMISSION,
    TREAT_PERMISSION,
    TreatmentError,
)


def _service() -> OperationalMonitorService:
    return OperationalMonitorService()


def _treatment_service() -> MonitorTreatmentService:
    return MonitorTreatmentService()


def _csrf_token() -> str:
    if "ofs_monitor_csrf" not in session:
        session["ofs_monitor_csrf"] = secrets.token_urlsafe(32)
    return session["ofs_monitor_csrf"]


def _csrf_valid() -> bool:
    expected = session.get("ofs_monitor_csrf") or ""
    supplied = request.headers.get("X-Monitor-CSRF") or ""
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def _treatment_failure(exc: TreatmentError):
    return jsonify({"ok": False, "error": {"code": exc.code, "message": str(exc)}, "current": exc.current}), exc.http_status


def _treatment_body():
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else {}


def _json_denied():
    return jsonify({"ok": False, "error": {"code": "FORBIDDEN", "message": "Acesso negado para este recurso."}}), 403


def _scope_from_request() -> str:
    body = request.get_json(silent=True) if request.method == "POST" else None
    return str((body or {}).get("scope") or request.args.get("scope") or "casa-cliente").strip().lower()


def init_app(app):
    @app.get("/ofs/monitor-operacional")
    @login_required
    @perm_required(PERMISSION)
    def ofs_operational_monitor_page():
        return render_template("ofs_operational_monitor.html", monitor_can_treat=has_perm(TREAT_PERMISSION),
                               monitor_can_supervise=has_perm(SUPERVISE_PERMISSION), monitor_csrf=_csrf_token())

    @app.get("/ofs/monitor-operacional/tratativas")
    @login_required
    def ofs_operational_monitor_treatments():
        if not has_perm(PERMISSION):
            return _json_denied()
        try:
            return jsonify({"ok": True, "data": _treatment_service().list_current(_scope_from_request())})
        except MonitorScopeError as exc:
            return jsonify({"ok": False, "error": {"code": "INVALID_SCOPE", "message": str(exc)}}), 400
        except Exception:
            current_app.logger.exception("Falha ao ler tratativas do monitor.")
            return jsonify({"ok": False, "error": {"code": "READ_ERROR", "message": "Não foi possível atualizar as tratativas."}}), 500

    @app.post("/ofs/monitor-operacional/tratativas/assumir")
    @login_required
    def ofs_operational_monitor_claim():
        if not has_perm(PERMISSION) or not has_perm(TREAT_PERMISSION):
            return _json_denied()
        if not _csrf_valid():
            return jsonify({"ok": False, "error": {"code": "INVALID_CSRF", "message": "Sessão expirada. Recarregue a página."}}), 403
        body = _treatment_body()
        try:
            data = _treatment_service().claim(
                str(body.get("scope") or "casa-cliente"), str(body.get("work_date") or ""),
                str(body.get("indicator") or ""), str(body.get("item_key") or ""), current_actor(),
            )
            return jsonify({"ok": True, "data": data})
        except TreatmentError as exc:
            return _treatment_failure(exc)
        except Exception:
            current_app.logger.exception("Falha ao reservar tratativa do monitor.")
            return jsonify({"ok": False, "error": {"code": "CLAIM_ERROR", "message": "Não foi possível reservar este caso."}}), 500

    @app.post("/ofs/monitor-operacional/tratativas/alterar")
    @login_required
    def ofs_operational_monitor_change():
        if not has_perm(PERMISSION) or not has_perm(TREAT_PERMISSION):
            return _json_denied()
        if not _csrf_valid():
            return jsonify({"ok": False, "error": {"code": "INVALID_CSRF", "message": "Sessão expirada. Recarregue a página."}}), 403
        body = _treatment_body()
        try:
            data = _treatment_service().change(
                str(body.get("scope") or "casa-cliente"), str(body.get("work_date") or ""),
                str(body.get("indicator") or ""), str(body.get("item_key") or ""), current_actor(),
                str(body.get("token") or ""), str(body.get("action") or ""), str(body.get("note") or ""),
            )
            return jsonify({"ok": True, "data": data})
        except TreatmentError as exc:
            return _treatment_failure(exc)
        except Exception:
            current_app.logger.exception("Falha ao alterar tratativa do monitor.")
            return jsonify({"ok": False, "error": {"code": "CHANGE_ERROR", "message": "Não foi possível salvar a tratativa."}}), 500

    @app.get("/ofs/monitor-operacional/supervisao")
    @login_required
    @perm_required(SUPERVISE_PERMISSION)
    def ofs_operational_monitor_supervision_page():
        return render_template("ofs_operational_monitor_supervision.html")

    @app.get("/ofs/monitor-operacional/supervisao/dados")
    @login_required
    def ofs_operational_monitor_supervision_data():
        if not has_perm(SUPERVISE_PERMISSION):
            return _json_denied()
        try:
            data = _treatment_service().supervision(
                request.args.get("scope") or "casa-cliente", request.args.get("preset") or "hour",
                int(request.args.get("top") or 5), request.args.get("start"), request.args.get("end"),
                request.args.get("action"), request.args.get("agent"),
            )
            return jsonify({"ok": True, "data": data})
        except ValueError as exc:
            if isinstance(exc, TreatmentError):
                return _treatment_failure(exc)
            return jsonify({"ok": False, "error": {"code": "INVALID_TOP", "message": "Quantidade de agentes inválida."}}), 400
        except MonitorScopeError as exc:
            return jsonify({"ok": False, "error": {"code": "INVALID_SCOPE", "message": str(exc)}}), 400
        except Exception:
            current_app.logger.exception("Falha ao ler supervisão do monitor.")
            return jsonify({"ok": False, "error": {"code": "READ_ERROR", "message": "Não foi possível carregar a supervisão."}}), 500

    @app.get("/ofs/monitor-operacional/data")
    @login_required
    def ofs_operational_monitor_data():
        if not has_perm(PERMISSION):
            return _json_denied()
        try:
            snapshot = _service().get_snapshot(_scope_from_request())
            return jsonify({"ok": True, "data": snapshot}), 200
        except MonitorScopeError as exc:
            return jsonify({"ok": False, "error": {"code": "INVALID_SCOPE", "message": str(exc)}}), 400
        except Exception:
            current_app.logger.exception("Falha ao ler snapshot do monitor operacional.")
            return jsonify({
                "ok": False,
                "error": {"code": "READ_MODEL_ERROR", "message": "Não foi possível ler o snapshot local."},
            }), 500

    @app.post("/ofs/monitor-operacional/refresh")
    @login_required
    def ofs_operational_monitor_refresh():
        if not has_perm(PERMISSION):
            return _json_denied()
        actor = current_actor()
        try:
            snapshot = _service().refresh(
                _scope_from_request(),
                actor_id=actor.get("id"),
                actor_username=actor.get("username"),
            )
            return jsonify({"ok": True, "data": snapshot}), 200
        except MonitorScopeError as exc:
            return jsonify({"ok": False, "error": {"code": "INVALID_SCOPE", "message": str(exc)}}), 400
        except MonitorRefreshCooldown as exc:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "REFRESH_COOLDOWN",
                    "message": str(exc),
                    "remaining_seconds": exc.snapshot.get("remaining_seconds", 0),
                },
                "data": exc.snapshot,
            }), 409
        except MonitorRefreshInProgress as exc:
            return jsonify({
                "ok": False,
                "error": {"code": "REFRESH_IN_PROGRESS", "message": str(exc)},
            }), 423
        except MonitorSourceUnavailable as exc:
            return jsonify({
                "ok": False,
                "error": {"code": "SOURCE_NOT_READY", "message": str(exc)},
            }), 409
        except Exception:
            current_app.logger.exception("Falha ao atualizar snapshot do monitor operacional.")
            return jsonify({
                "ok": False,
                "error": {
                    "code": "REFRESH_FAILED",
                    "message": "Não foi possível atualizar o snapshot local. O último snapshot válido foi preservado.",
                },
            }), 500
