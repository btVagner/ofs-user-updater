"""Run tests with DB import blocked; optional isolated-MySQL tests use explicit factory."""
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def forbidden_connection():
    raise AssertionError('Teste tentou usar o banco configurado no projeto. Use uma factory isolada.')


def main():
    # Existing routes eagerly import the production pool. Avoid that side effect.
    module = types.ModuleType('database.connection')
    module.get_connection = forbidden_connection
    sys.modules['database.connection'] = module
    import pytest
    args = sys.argv[1:] or ['tests/test_ofs_repeated_orders.py', 'tests/test_ofs_repeated_orders_routes.py']
    return pytest.main([*args, '--basetemp', str(ROOT / '.pytest_cache' / 'repeated-orders-temp')])


if __name__ == '__main__':
    raise SystemExit(main())
