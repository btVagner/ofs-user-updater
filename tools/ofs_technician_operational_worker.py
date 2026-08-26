import argparse
import json
import logging
import signal
import sys
import threading
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from services.ofs_technician_operational_service import (  # noqa: E402
    MySQLOperationalRepository,
    OperationalAlreadyRunning,
    OperationalSettings,
    TechnicianOperationalCollector,
    mysql_operational_lock,
    sanitize_operational_error,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Worker independente do read model operacional de técnicos OFS.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--baseline-once", action="store_true", help="Executa baseline/recovery completo uma vez e encerra.")
    mode.add_argument("--events-once", action="store_true", help="Lê/drena Events até o cursor atual e encerra.")
    mode.add_argument("--status", action="store_true", help="Exibe apenas métricas locais MySQL; não consulta OFS.")
    parser.add_argument("--date", default=None, help="Data YYYY-MM-DD para baseline/status. Padrão: hoje.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    target_date = date.fromisoformat(args.date) if args.date else date.today()
    repository = MySQLOperationalRepository()

    if args.status:
        print(json.dumps(repository.operational_metrics(target_date), ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    settings = OperationalSettings.from_env()
    collector = TechnicianOperationalCollector(repository=repository, settings=settings)

    try:
        with mysql_operational_lock():
            if args.baseline_once:
                result = collector.run_baseline(target_date)
                result["metrics"] = repository.operational_metrics(target_date)
                print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
                return 0
            if args.events_once:
                collector.refresh_technicians()
                result = collector.drain_events()
                result["metrics"] = repository.operational_metrics(target_date)
                print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
                return 0

            stop_event = threading.Event()
            interrupt_count = 0

            def request_stop(signum, frame):
                nonlocal interrupt_count
                interrupt_count += 1
                if interrupt_count == 1:
                    print("[OFS_OPERATIONAL_WORKER] encerramento solicitado; aguardando ciclo em andamento finalizar")
                    stop_event.set()
                    return
                raise KeyboardInterrupt

            previous_sigint = signal.signal(signal.SIGINT, request_stop)
            previous_sigterm = None
            if hasattr(signal, "SIGTERM"):
                previous_sigterm = signal.signal(signal.SIGTERM, request_stop)
            try:
                collector.run_forever(stop_predicate=stop_event.is_set)
            finally:
                signal.signal(signal.SIGINT, previous_sigint)
                if previous_sigterm is not None:
                    signal.signal(signal.SIGTERM, previous_sigterm)
            print("[OFS_OPERATIONAL_WORKER] encerrado graciosamente")
            return 0
    except OperationalAlreadyRunning as exc:
        print(f"[OFS_OPERATIONAL_WORKER] {exc}")
        return 3
    except KeyboardInterrupt:
        print("[OFS_OPERATIONAL_WORKER] encerrado pelo operador")
        return 0
    except Exception as exc:
        print(f"[OFS_OPERATIONAL_WORKER] ERRO: {sanitize_operational_error(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
