from pathlib import Path
import pytest
from flask import Flask

from services.ofs_repeated_orders_service import VIEW_PERMISSION, REFRESH_PERMISSION, RefreshRejected
import routes.ofs_repeated_orders_routes as routes


class Repository:
    def __init__(self):
        self.reads, self.requests = 0, []

    def read(self):
        self.reads += 1
        return {'payload':None, 'run':None}

    def request_refresh(self, actor):
        self.requests.append(actor)
        return {'id':1, 'status':'queued'}


def app_client(monkeypatch, permissions, repo=None):
    app = Flask(__name__)
    app.secret_key = 'unit-test'
    app.testing = True
    app.add_url_rule('/login', endpoint='login', view_func=lambda: 'login')
    app.add_url_rule('/', endpoint='home', view_func=lambda: 'home')
    app.jinja_env.globals['has_perm'] = routes.has_perm
    monkeypatch.setattr(routes, '_repository', lambda: repo or Repository())
    monkeypatch.setattr(routes, 'render_template', lambda name, **kw: str(kw))
    routes.init_app(app)
    client = app.test_client()
    if permissions is not None:
        with client.session_transaction() as session:
            session.update(usuario_logado='tester', usuario_id=7, tipo_id=2, permissoes=permissions,
                           repeated_orders_csrf='test-csrf')
    return client


@pytest.mark.parametrize('path,method', [('/ofs/os-repetidas','get'),('/ofs/os-repetidas/data','get'),('/ofs/os-repetidas/refresh','post')])
def test_login_required(monkeypatch, path, method):
    response = getattr(app_client(monkeypatch, None), method)(path)
    assert response.status_code == 302 and '/login' in response.location


def test_view_permission_is_separate_and_get_does_not_collect(monkeypatch):
    repo = Repository()
    client = app_client(monkeypatch, [VIEW_PERMISSION], repo)
    assert 'False' in client.get('/ofs/os-repetidas').text
    assert client.get('/ofs/os-repetidas/data?contract=123').status_code == 200
    assert repo.reads == 1 and repo.requests == []
    assert client.post('/ofs/os-repetidas/refresh', headers={'X-Repeated-Orders-CSRF':'test-csrf'}).status_code == 403


@pytest.mark.parametrize('perms', [[], ['ofs.monitor_operacional'], [REFRESH_PERMISSION]])
def test_no_accidental_permission_grants(monkeypatch, perms):
    client = app_client(monkeypatch, perms)
    assert client.get('/ofs/os-repetidas').status_code == 302
    assert client.get('/ofs/os-repetidas/data').status_code == 403
    assert client.post('/ofs/os-repetidas/refresh').status_code == 403


def test_csrf_and_actor_and_async_acceptance(monkeypatch):
    repo = Repository()
    client = app_client(monkeypatch, [VIEW_PERMISSION, REFRESH_PERMISSION], repo)
    assert client.post('/ofs/os-repetidas/refresh').status_code == 403
    assert client.post('/ofs/os-repetidas/refresh', headers={'X-Repeated-Orders-CSRF':'wrong'}).status_code == 403
    response = client.post('/ofs/os-repetidas/refresh', headers={'X-Repeated-Orders-CSRF':'test-csrf'})
    assert response.status_code == 202
    assert response.json['data']['status'] == 'queued'
    assert repo.requests[0]['id'] == 7 and repo.requests[0]['username'] == 'tester'


def test_cooldown_and_error_are_safe(monkeypatch):
    repo = Repository()
    def reject(_):
        raise RefreshRejected('COOLDOWN', 100)
    repo.request_refresh = reject
    client = app_client(monkeypatch, [VIEW_PERMISSION, REFRESH_PERMISSION], repo)
    response = client.post('/ofs/os-repetidas/refresh', headers={'X-Repeated-Orders-CSRF':'test-csrf'})
    assert response.status_code == 409 and response.json['error']['remaining_seconds'] == 100
    def broken():
        raise RuntimeError('secret-test-error')
    repo.read = broken
    response = client.get('/ofs/os-repetidas/data')
    assert response.status_code == 503 and 'secret-test-error' not in response.text


def test_navigation_order_and_safe_frontend():
    root = Path(__file__).resolve().parents[1]
    navbar = (root/'templates/includes/navbar.html').read_text(encoding='utf-8')
    assert navbar.index('Monitor operacional</span>') < navbar.index('OS repetidas</span>') < navbar.index('BI Atividades OFS</span>')
    assert VIEW_PERMISSION in (root/'templates/home.html').read_text(encoding='utf-8')
    js = (root/'static/js/ofs_repeated_orders.js').read_text(encoding='utf-8')
    assert 'innerHTML' not in js and 'textContent' in js
    assert '/rest/ofsc' not in js and 'pageSize = 30' in js
