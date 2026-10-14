"""Explicit schema operations; never run during import or app startup."""
import argparse
import json
import sys
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / '.env')
SQL = ROOT / 'database' / 'sql'
CONFIRM = 'DROP_OFS_REPEATED_ORDERS'
EXPECTED = {
    'ofs_repeated_orders_control': {'id', 'last_requested_at', 'last_started_at'},
    'ofs_repeated_orders_run': {'id', 'status', 'requested_at', 'requested_by_user_id',
        'requested_by_username', 'started_at', 'heartbeat_at', 'finished_at', 'date_from',
        'date_to', 'pages', 'received', 'error_text'},
    'ofs_repeated_orders_snapshot': {'id', 'run_id', 'payload_json', 'published_at'},
}


def validate(cur):
    for table, columns in EXPECTED.items():
        cur.execute('SELECT ENGINE AS engine_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s', (table,))
        row = cur.fetchone()
        if not row or row['engine_name'].upper() != 'INNODB':
            raise RuntimeError(f'Tabela ausente ou engine incorreta: {table}')
        cur.execute('SELECT COLUMN_NAME AS field_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s', (table,))
        missing = columns - {row['field_name'] for row in cur.fetchall()}
        if missing:
            raise RuntimeError(f'Colunas ausentes em {table}: {sorted(missing)}')
        cur.execute('SELECT COLUMN_NAME AS field_name FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=%s AND index_name=%s', (table, 'PRIMARY'))
        if [row['field_name'] for row in cur.fetchall()] != ['id']:
            raise RuntimeError(f'Chave primária incorreta em {table}')
    cur.execute('SELECT COUNT(*) AS total FROM ofs_repeated_orders_control WHERE id=1')
    if cur.fetchone()['total'] != 1:
        raise RuntimeError('Controle singleton ausente.')
    cur.execute("SELECT recurso FROM permissoes WHERE recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar')")
    if len(cur.fetchall()) != 2:
        raise RuntimeError('Permissões ausentes ou duplicadas.')


def run(mode, factory=None):
    if factory is None:
        from database.connection import get_connection
        factory = get_connection
    conn = factory()
    cur = conn.cursor(dictionary=True)
    locked = False
    worker_locked = False
    try:
        # Schema processes serialize separately from the collector's lock.
        cur.execute("SELECT GET_LOCK('ofs_repeated_orders_schema',0) AS acquired")
        if cur.fetchone()['acquired'] != 1:
            raise RuntimeError('Outra migração está ativa.')
        locked = True
        if mode == 'rollback':
            cur.execute("SELECT GET_LOCK('ofs_repeated_orders_worker',0) AS acquired")
            worker_locked = cur.fetchone()['acquired'] == 1
            if not worker_locked:
                raise RuntimeError('Worker ativo. Interrompa-o antes do rollback.')
        path = SQL / f'20261014_ofs_repeated_orders_{mode}.sql'
        cleaned = '\n'.join(line for line in path.read_text(encoding='utf-8').splitlines()
                            if not line.strip().startswith('--'))
        results = []
        for statement in cleaned.split(';'):
            if not statement.strip():
                continue
            cur.execute(statement)
            if cur.with_rows:
                results.append(cur.fetchall())
        if mode in {'apply', 'validate'}:
            validate(cur)
        conn.commit()
        return {'mode': mode, 'validated': mode != 'rollback', 'results': results}
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            if worker_locked:
                cur.execute("SELECT RELEASE_LOCK('ofs_repeated_orders_worker')")
                cur.fetchone()
            if locked:
                cur.execute("SELECT RELEASE_LOCK('ofs_repeated_orders_schema')")
                cur.fetchone()
        finally:
            cur.close()
            conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    for mode in ('apply', 'validate', 'rollback'):
        modes.add_argument('--' + mode, action='store_true')
    parser.add_argument('--confirm')
    args = parser.parse_args()
    if args.rollback and args.confirm != CONFIRM:
        parser.error(f'Rollback exige --confirm {CONFIRM}, backup e web/worker interrompidos.')
    mode = next(mode for mode in ('apply', 'validate', 'rollback') if getattr(args, mode))
    print(json.dumps(run(mode), default=str, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
