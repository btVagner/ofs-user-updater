import hmac
import secrets

from flask import current_app, jsonify, render_template, request, session
from core.auth import current_actor, has_perm, login_required, perm_required
from services.ofs_repeated_orders_service import (
    VIEW_PERMISSION, REFRESH_PERMISSION, RefreshRejected, RepeatedOrdersRepository,
)


def _repository():
    return RepeatedOrdersRepository()


def failure(code, message, status):
    return jsonify(ok=False, error={"code": code, "message": message}), status


def init_app(app):
    @app.get('/ofs/os-repetidas')
    @login_required
    @perm_required(VIEW_PERMISSION)
    def ofs_repeated_orders_page():
        session.setdefault('repeated_orders_csrf', secrets.token_urlsafe(32))
        return render_template('ofs_repeated_orders.html', can_refresh=has_perm(REFRESH_PERMISSION),
                               csrf=session['repeated_orders_csrf'])

    @app.get('/ofs/os-repetidas/data')
    @login_required
    def ofs_repeated_orders_data():
        if not has_perm(VIEW_PERMISSION):
            return failure('FORBIDDEN', 'Acesso negado.', 403)
        try:
            response = jsonify(ok=True, data=_repository().read())
            response.headers['Cache-Control'] = 'no-store'
            return response
        except Exception:
            current_app.logger.exception('Falha ao ler snapshot OS repetidas.')
            return failure('READ_ERROR', 'Não foi possível ler o snapshot. Verifique a implantação.', 503)

    @app.post('/ofs/os-repetidas/refresh')
    @login_required
    def ofs_repeated_orders_refresh():
        if not has_perm(VIEW_PERMISSION) or not has_perm(REFRESH_PERMISSION):
            return failure('FORBIDDEN', 'Acesso negado.', 403)
        expected = session.get('repeated_orders_csrf', '')
        supplied = request.headers.get('X-Repeated-Orders-CSRF', '')
        if not expected or not supplied or not hmac.compare_digest(expected, supplied):
            return failure('INVALID_CSRF', 'Recarregue a página para atualizar sua sessão.', 403)
        try:
            return jsonify(ok=True, data=_repository().request_refresh(current_actor())), 202
        except RefreshRejected as exc:
            return jsonify(ok=False, error={"code": exc.code, "message": str(exc),
                                            "remaining_seconds": exc.seconds}), 409
        except Exception:
            current_app.logger.exception('Falha ao agendar atualização OS repetidas.')
            return failure('QUEUE_ERROR', 'Não foi possível agendar a atualização.', 503)
