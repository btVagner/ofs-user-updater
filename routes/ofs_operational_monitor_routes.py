from __future__ import annotations

from flask import current_app, jsonify, render_template, request

from core.auth import current_actor, has_perm, login_required, perm_required
from services.ofs_operational_monitor_service import (
    PERMISSION,
    MonitorRefreshCooldown,
    MonitorRefreshInProgress,
    MonitorScopeError,
    MonitorSourceUnavailable,
    OperationalMonitorService,
)


def _service() -> OperationalMonitorService:
    return OperationalMonitorService()


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
        return render_template("ofs_operational_monitor.html")

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
