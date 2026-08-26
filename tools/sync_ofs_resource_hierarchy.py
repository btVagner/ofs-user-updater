import argparse
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from services.ofs_resource_hierarchy_service import (  # noqa: E402
    HierarchySyncAlreadyRunning,
    sync_resource_hierarchy,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sincroniza a hierarquia de recursos OFS no MySQL local."
    )
    parser.add_argument(
        "--root",
        dest="root_resource_id",
        default=None,
        help="Root OFS opcional. Por padrão usa OFS_ROOT_RESOURCE_ID/configuração central.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    started = time.perf_counter()

    try:
        result = sync_resource_hierarchy(root_resource_id=args.root_resource_id)
    except HierarchySyncAlreadyRunning as exc:
        print(f"[OFS_HIERARCHY] {exc}")
        return 0
    except Exception as exc:
        print(f"[OFS_HIERARCHY] ERRO: {exc}")
        return 1

    elapsed = time.perf_counter() - started
    print("[OFS_HIERARCHY] Sincronização concluída.")
    print(f"root_resource_id: {result['root_resource_id']}")
    print(f"resources_total: {result['resources_total']}")
    print(f"descendants_total: {result['descendants_total']}")
    print(f"active_total: {result['active_total']}")
    print(f"inactive_total: {result['inactive_total']}")
    print(f"max_depth: {result['max_depth']}")
    print(f"depth_counts: {result['depth_counts']}")
    print(f"resource_type_counts: {result['resource_type_counts']}")
    print(f"pages: {result['pages']}")
    print(f"api_calls: {result['api_calls']}")
    print(f"removed_total: {result['removed_total']}")
    print(f"fields_fallback_used: {result['fields_fallback_used']}")
    print(f"skipped_without_resource_id: {result['skipped_without_resource_id']}")
    print(f"skipped_inactive_status: {result['skipped_inactive_status']}")
    print(
        "skipped_under_inactive_ancestor: "
        f"{result['skipped_under_inactive_ancestor']}"
    )
    print(f"status_rechecks_total: {result['status_rechecks_total']}")
    print(
        "inactive_confirmed_by_recheck: "
        f"{result['inactive_confirmed_by_recheck']}"
    )
    print(f"seen_at_utc: {result['seen_at_utc']}")
    print(f"elapsed_seconds: {elapsed:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
