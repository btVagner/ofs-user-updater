"""Run separately from Gunicorn. No automatic refresh: consumes authorized requests."""
import argparse
import json
import signal
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / '.env')

from services.ofs_repeated_orders_service import RepeatedOrdersRepository, RefreshRejected, run_worker_once


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--once', action='store_true', help='Consome uma solicitação pendente e encerra.')
    mode.add_argument('--status', action='store_true', help='Somente MySQL; nunca consulta OFS.')
    args = parser.parse_args()
    repository = RepeatedOrdersRepository()
    if args.status:
        state = repository.read()
        state.pop('payload', None)
        print(json.dumps(state, ensure_ascii=False, indent=2))
        return 0
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    while not stop.is_set():
        try:
            run_worker_once(repository)
        except RefreshRejected:
            pass  # Other process owns the global worker lock.
        if args.once:
            break
        stop.wait(5)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
