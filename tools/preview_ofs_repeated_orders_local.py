"""Fixture-only preview. No project DB, no OFS, bound exclusively to loopback."""
import argparse
import re
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
module = types.ModuleType('database.connection')
def blocked():
    raise AssertionError('Fixture preview cannot access project DB.')
module.get_connection = blocked
sys.modules['database.connection'] = module

from flask import Flask, jsonify, redirect, session
from services.ofs_repeated_orders_service import VIEW_PERMISSION, REFRESH_PERMISSION, analyze, operational_today
import routes.ofs_repeated_orders_routes as routes


def create_fixture_app():
    app = Flask(__name__, template_folder=str(ROOT/'templates'), static_folder=str(ROOT/'static'))
    app.secret_key = 'local-fixture-only'
    rows = []
    for i in range(37):
        for j in range(3 if i < 4 else 2):
            rows.append(dict(activityId=f'{i}-{j}', customerNumber=f'{i:07}', apptNumber=f'OS{i}{j}-A',
                XA_ORG_SYS='NG', date=(operational_today()+timedelta(days=j if i==0 else 0)).isoformat(), activityType='MAN' if i % 2 and j else 'INST',
                status='pending', recordType='regular', resourceId='TEC-ALT' if i==0 and j==1 else f'TEC-{i}',
                stateProvince='' if i==36 else 'RS' if j==1 else 'MG', timeSlot='08:00-12:00' if j==0 else '13:00-18:00'))
    for j in range(2):
        rows.append({**rows[j], 'customerNumber':'<img src=x onerror=alert(1)>', 'activityId':f'XSS{j}'})
    rows.append({**rows[0], 'apptNumber':'OS00-B', 'activityId':'LINKED-0'})
    rows.append(dict(rows[0]))  # Exact duplicate must not inflate activity totals.
    now = datetime.now(timezone.utc).isoformat()
    class FixtureRepository:
        def read(self):
            payload = analyze(rows, operational_today())
            payload['regional_source'] = 'activity.stateProvince'
            return dict(run={'id':1,'status':'succeeded'}, payload=payload,
                        published_at=now, snapshot_run_id=1, remaining_seconds=0, stale=False)
        def request_refresh(self, actor):
            return {'id':2,'status':'queued'}
    routes._repository = FixtureRepository
    routes.init_app(app)
    app.jinja_env.globals['has_perm'] = routes.has_perm
    for path in ('templates/includes/navbar.html', 'templates/includes/head.html', 'templates/home.html'):
        for endpoint in set(re.findall(r"url_for\('([^']+)'", (ROOT/path).read_text(encoding='utf-8'))):
            if endpoint not in app.view_functions:
                app.add_url_rule('/fixture-link/'+endpoint, endpoint=endpoint, view_func=lambda: jsonify(ok=True))
    @app.get('/fixture-session')
    def fixture_session():
        session.update(usuario_logado='fixture', nome_usuario='Validação local', usuario_id=1, tipo_id=2,
                       permissoes=[VIEW_PERMISSION, REFRESH_PERMISSION, 'ofs.monitor_operacional'])
        return redirect('/ofs/os-repetidas')
    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=5097)
    args = parser.parse_args()
    create_fixture_app().run(host='127.0.0.1', port=args.port, debug=False, use_reloader=False)
