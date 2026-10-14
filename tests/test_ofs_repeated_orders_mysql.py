"""Opt-in integration tests against disposable MySQL; never read project DB env."""
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date
from multiprocessing import get_context

import pytest
from services.ofs_repeated_orders_service import (
    RepeatedOrdersRepository, RefreshRejected, RepeatedOrdersError, analyze, run_worker_once,
)
from tools.ofs_repeated_orders_schema import run, validate

pytestmark = pytest.mark.skipif(os.getenv('OFS_REPEAT_DISPOSABLE_MYSQL') != '1',
                               reason='Requires explicitly opted-in disposable MySQL on localhost:33317')


def mysql_factory():
    import mysql.connector
    # Fixed test target; neither load_dotenv nor DB_* may change this destination.
    return mysql.connector.connect(host='127.0.0.1', port=33317, user='root',
        password='local-repeated-orders-test', database='ofs_repeat_test', autocommit=False)


def enqueue_process(number):
    try:
        return RepeatedOrdersRepository(mysql_factory).request_refresh({'id':number, 'username':f'worker-{number}'})['status']
    except RefreshRejected as exc:
        return exc.code


@pytest.fixture
def repository():
    conn = mysql_factory()
    cur = conn.cursor()
    for table in ('ofs_repeated_orders_snapshot', 'ofs_repeated_orders_run', 'ofs_repeated_orders_control',
                  'perfil_permissao', 'permissoes', 'perfis'):
        cur.execute(f'DROP TABLE IF EXISTS {table}')
    cur.execute('CREATE TABLE permissoes(id INT AUTO_INCREMENT PRIMARY KEY,recurso VARCHAR(100) UNIQUE,descricao VARCHAR(255)) ENGINE=InnoDB')
    cur.execute('CREATE TABLE perfis(id INT PRIMARY KEY,slug VARCHAR(100)) ENGINE=InnoDB')
    cur.execute('CREATE TABLE perfil_permissao(perfil_id INT,permissao_id INT,PRIMARY KEY(perfil_id,permissao_id)) ENGINE=InnoDB')
    cur.execute("INSERT INTO perfis VALUES(1,'admin'),(2,'reader')")
    conn.commit()
    cur.close()
    conn.close()
    run('apply', mysql_factory)
    return RepeatedOrdersRepository(mysql_factory)


def sql(statement):
    conn = mysql_factory()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(statement)
        rows = cur.fetchall() if cur.with_rows else []
        conn.commit()
        return rows
    finally:
        cur.close(); conn.close()


def test_apply_validate_idempotent_and_rollback(repository):
    assert run('apply', mysql_factory)['validated']
    assert run('validate', mysql_factory)['validated']
    assert sql('SELECT COUNT(*) AS n FROM permissoes')[0]['n'] == 2
    assert sql('SELECT COUNT(*) AS n FROM perfil_permissao')[0]['n'] == 2
    with repository.worker_lock():
        with pytest.raises(RuntimeError, match='Worker ativo'):
            run('rollback', mysql_factory)
    run('rollback', mysql_factory)
    assert sql('SELECT COUNT(*) AS n FROM permissoes')[0]['n'] == 0
    assert run('apply', mysql_factory)['validated']


def test_requests_serialize_across_processes_and_cooldown(repository):
    with ProcessPoolExecutor(max_workers=4, mp_context=get_context('spawn')) as pool:
        results = list(pool.map(enqueue_process, range(8)))
    assert results.count('queued') == 1
    assert results.count('BUSY') == 7
    assert sql('SELECT COUNT(*) AS n FROM ofs_repeated_orders_run')[0]['n'] == 1
    sql("UPDATE ofs_repeated_orders_run SET status='failed'")
    with pytest.raises(RefreshRejected) as exc:
        repository.request_refresh({'id':1})
    assert exc.value.code == 'COOLDOWN'


def test_worker_mutual_exclusion_and_lost_lock_fencing(repository):
    with repository.worker_lock() as (conn, cur):
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(RefreshRejected):
                pool.submit(lambda: next_lock(repository)).result()
        cur.execute("SELECT RELEASE_LOCK('ofs_repeated_orders_worker')")
        cur.fetchone()
        with pytest.raises(RepeatedOrdersError, match='lock'):
            repository.ensure_owner(cur)


def next_lock(repository):
    with repository.worker_lock():
        return True


def test_recovery_preserves_snapshot_and_audits_interruption(repository):
    repository.request_refresh({'id':7, 'username':'fixture'})
    with repository.worker_lock() as (conn, cur):
        run_id = repository.claim(conn, cur, date(2026, 10, 14))
        repository.finish(conn, cur, run_id, payload=analyze([], date(2026, 10, 14)))
    original = repository.read()
    sql("UPDATE ofs_repeated_orders_control SET last_requested_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE,last_started_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE")
    repository.request_refresh({'id':7})
    with repository.worker_lock() as (conn, cur):
        run_id = repository.claim(conn, cur, date(2026, 10, 14))
        repository.progress(conn, cur, run_id, 2, 13)
    # Connection closes without finish, simulating process death.
    assert not run_worker_once(repository, lambda: pytest.fail('Must not retry OFS automatically'))
    current = repository.read()
    assert current['run']['status'] == 'interrupted'
    assert current['run']['pages'] == 2
    assert current['run']['finished_at']
    assert current['payload'] == original['payload']
    assert current['snapshot_run_id'] == original['snapshot_run_id']


def test_partial_failure_keeps_old_snapshot(repository):
    class API:
        def __init__(self): self.calls = 0
        def _request(self, method, path, **_kw):
            if path == 'resources/02/descendants': return {'items':[], 'totalResults':0}
            self.calls += 1
            if self.calls > 1: raise RuntimeError('page failure')
            return {'items':[], 'hasMore':False}
    repository.request_refresh({'id':1})
    assert run_worker_once(repository, API)
    before = repository.read()
    assert before['run']['status'] == 'succeeded'
    sql("UPDATE ofs_repeated_orders_control SET last_requested_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE,last_started_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE")
    repository.request_refresh({'id':2})
    class BrokenAPI(API):
        def _request(self, method, path, **_kw):
            if path == 'resources/02/descendants': return {'items':[], 'totalResults':0}
            self.calls += 1
            if self.calls > 1: raise RuntimeError('page failure')
            return {'items':[{'activityId':'fixture'}], 'hasMore':True}
    assert run_worker_once(repository, BrokenAPI)
    after = repository.read()
    assert after['payload'] == before['payload']
    assert after['snapshot_run_id'] == before['snapshot_run_id']
    assert after['run']['status'] == 'failed' and after['run']['pages'] == 1


def test_queued_delay_cannot_bypass_start_interval(repository):
    repository.request_refresh({'id':1})
    sql("UPDATE ofs_repeated_orders_control SET last_requested_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE,last_started_at=UTC_TIMESTAMP()-INTERVAL 1 MINUTE")
    with repository.worker_lock() as (conn, cur):
        assert repository.claim(conn, cur, date(2026, 10, 14)) is None
    assert repository.read()['run']['status'] == 'queued'
    sql("UPDATE ofs_repeated_orders_control SET last_started_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE")
    with repository.worker_lock() as (conn, cur):
        assert repository.claim(conn, cur, date(2026, 10, 14)) == 1
        assert repository.read()['run']['status'] == 'running'
        repository.finish(conn, cur, 1, error='fixture finish')


def test_publication_rollback_is_atomic(repository):
    repository.request_refresh({'id':1})
    with repository.worker_lock() as (conn, cur):
        run_id = repository.claim(conn, cur, date(2026, 10, 14))
        repository.finish(conn, cur, run_id, payload=analyze([], date(2026, 10, 14)))
    original = repository.read()
    sql("UPDATE ofs_repeated_orders_control SET last_requested_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE,last_started_at=UTC_TIMESTAMP()-INTERVAL 11 MINUTE")
    repository.request_refresh({'id':2})
    with repository.worker_lock() as (conn, cur):
        run_id = repository.claim(conn, cur, date(2026, 10, 14))
        class FailingCursor:
            def execute(self, statement, params=None):
                if "SET status=%s,error_text" in statement:
                    raise RuntimeError('forced transaction failure')
                return cur.execute(statement, params)
            def fetchone(self): return cur.fetchone()
        with pytest.raises(RuntimeError, match='forced'):
            repository.finish(conn, FailingCursor(), run_id, payload=analyze([], date(2026, 10, 15)))
    assert repository.read()['snapshot_run_id'] == original['snapshot_run_id']
    assert repository.read()['payload'] == original['payload']


def test_validate_detects_schema_drift(repository):
    sql('ALTER TABLE ofs_repeated_orders_control DROP COLUMN last_started_at')
    with pytest.raises(RuntimeError, match='Colunas ausentes'):
        run('validate', mysql_factory)
