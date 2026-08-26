import sys
import types
import unittest
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import requests


if "database.connection" not in sys.modules:
    fake_connection_module = types.ModuleType("database.connection")
    fake_connection_module.get_connection = lambda: (_ for _ in ()).throw(
        AssertionError("conexão real não deve ser usada neste teste")
    )
    sys.modules["database.connection"] = fake_connection_module

from services import ofs_resource_hierarchy_service as service  # noqa: E402


class FakeOFSClient:
    base_url = "https://ofs.example/rest/ofscCore/v1"

    def __init__(
        self,
        root=None,
        descendants=None,
        fail_offset=None,
        reject_fields=False,
        resource_details=None,
    ):
        self.root = root or {
            "resourceId": "02",
            "name": "Raiz OFS",
            "resourceType": "GR",
            "status": "active",
            "timeZone": "America/Sao_Paulo",
        }
        self.descendants = list(descendants or [])
        self.fail_offset = fail_offset
        self.reject_fields = reject_fields
        self.resource_details = dict(resource_details or {})
        self.calls = []

    def authenticated_get(self, url):
        self.calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if self.reject_fields and "fields" in query:
            response = requests.Response()
            response.status_code = 400
            raise requests.HTTPError("fields rejected", response=response)

        if parsed.path.endswith("/resources/02"):
            return dict(self.root)

        for resource_id, payload in self.resource_details.items():
            if parsed.path.endswith("/resources/" + resource_id):
                return dict(payload)

        if parsed.path.endswith("/resources/02/descendants"):
            offset = int((query.get("offset") or ["0"])[0])
            limit = int((query.get("limit") or ["100"])[0])
            if self.fail_offset is not None and offset >= self.fail_offset:
                raise requests.Timeout("simulated timeout")
            return {
                "totalResults": len(self.descendants),
                "limit": limit,
                "offset": offset,
                "items": self.descendants[offset:offset + limit],
            }

        response = requests.Response()
        response.status_code = 404
        raise requests.HTTPError(f"not found: {url}", response=response)


class MemoryRepository:
    def __init__(self):
        self.rows = {}
        self.replace_calls = 0

    def replace_snapshot(self, rows, root_resource_id, seen_at):
        self.replace_calls += 1
        incoming = {row["resource_id"]: dict(row) for row in rows}
        removed = 0
        for resource_id, row in list(self.rows.items()):
            if row["root_resource_id"] == root_resource_id and resource_id not in incoming:
                del self.rows[resource_id]
                removed += 1
        self.rows.update(incoming)
        return removed


def resource(resource_id, parent, *, kind="BK", status="active", name=None, timezone=None):
    item = {
        "resourceId": resource_id,
        "parentResourceId": parent,
        "resourceType": kind,
        "status": status,
        "name": name or resource_id,
    }
    if timezone:
        item["timeZone"] = timezone
    return item


class ResourceHierarchyTests(unittest.TestCase):
    def test_simple_tree_uses_parent_resource_id(self):
        client = FakeOFSClient(descendants=[resource("BK1", "02")])
        snapshot = service.fetch_resource_hierarchy_snapshot(client=client, root_resource_id="02")
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertEqual(rows["02"]["depth"], 0)
        self.assertEqual(rows["BK1"]["parent_resource_id"], "02")
        self.assertEqual(rows["BK1"]["depth"], 1)
        self.assertEqual(snapshot["resources_total"], 2)

    def test_multiple_levels_are_generic_not_fixed(self):
        descendants = [
            resource("REG", "02", kind="GR"),
            resource("CLUSTER", "REG", kind="GR"),
            resource("BK1", "CLUSTER", kind="BK"),
            resource("TEC1", "BK1", kind="TCV"),
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        depths = {row["resource_id"]: row["depth"] for row in snapshot["rows"]}
        self.assertEqual(depths, {"02": 0, "REG": 1, "CLUSTER": 2, "BK1": 3, "TEC1": 4})

    def test_pagination_filters_cadastral_inactive_resource(self):
        descendants = [
            resource(
                f"R{i:03d}",
                "02",
                status="inactive" if i == 204 else "active",
                timezone="America/Sao_Paulo" if i == 203 else None,
            )
            for i in range(205)
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertEqual(snapshot["pages"], 3)
        self.assertEqual(snapshot["api_calls"], 4)  # root + 3 páginas
        self.assertEqual(snapshot["descendants_total"], 205)
        self.assertEqual(snapshot["resources_total"], 205)  # root + 204 ativos
        self.assertNotIn("R204", rows)
        self.assertEqual(snapshot["skipped_inactive_status"], 1)
        self.assertEqual(rows["R203"]["timezone"], "America/Sao_Paulo")

    def test_parent_change_is_upserted_without_duplicate(self):
        repo = MemoryRepository()
        fixed = datetime(2026, 8, 25, 18, 0, 0)

        service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("A", "02"), resource("B", "A")]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
            seen_at=fixed,
        )
        self.assertEqual(repo.rows["B"]["parent_resource_id"], "A")

        changed_b = resource(
            "B",
            "02",
            kind="TCP",
            status="active",
            name="Técnico B atualizado",
            timezone="America/Sao_Paulo",
        )
        service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("A", "02"), changed_b]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
            seen_at=fixed,
        )
        self.assertEqual(repo.rows["B"]["parent_resource_id"], "02")
        self.assertEqual(repo.rows["B"]["resource_name"], "Técnico B atualizado")
        self.assertEqual(repo.rows["B"]["resource_type"], "TCP")
        self.assertEqual(repo.rows["B"]["status"], "active")
        self.assertEqual(repo.rows["B"]["timezone"], "America/Sao_Paulo")
        self.assertEqual(len(repo.rows), 3)

        summary = service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("A", "02"), changed_b]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
            seen_at=fixed,
        )
        self.assertEqual(summary["depth_counts"], {"0": 1, "1": 2})
        self.assertEqual(summary["resource_type_counts"]["TCP"], 1)

    def test_repeated_upsert_is_idempotent(self):
        repo = MemoryRepository()
        client = FakeOFSClient(descendants=[resource("A", "02"), resource("B", "A")])

        for second in (0, 1):
            service.sync_resource_hierarchy(
                client=client,
                root_resource_id="02",
                repository=repo,
                use_lock=False,
                seen_at=datetime(2026, 8, 25, 18, 0, second),
            )

        self.assertEqual(len(repo.rows), 3)
        self.assertEqual(repo.replace_calls, 2)

    def test_missing_resource_is_removed_only_after_complete_snapshot(self):
        repo = MemoryRepository()
        service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("A", "02"), resource("B", "02")]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
        )
        result = service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("A", "02")]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
        )
        self.assertEqual(result["removed_total"], 1)
        self.assertNotIn("B", repo.rows)

    def test_failure_in_middle_does_not_touch_database_snapshot(self):
        repo = MemoryRepository()
        descendants = [resource(f"R{i:03d}", "02") for i in range(150)]
        client = FakeOFSClient(descendants=descendants, fail_offset=100)

        with self.assertRaises(requests.Timeout):
            service.sync_resource_hierarchy(
                client=client,
                root_resource_id="02",
                repository=repo,
                use_lock=False,
            )

        self.assertEqual(repo.replace_calls, 0)
        self.assertEqual(repo.rows, {})

    def test_fields_compatibility_fallback(self):
        client = FakeOFSClient(
            descendants=[resource("A", "02")],
            reject_fields=True,
        )
        snapshot = service.fetch_resource_hierarchy_snapshot(client=client, root_resource_id="02")
        self.assertTrue(snapshot["fields_fallback_used"])
        self.assertEqual(snapshot["resources_total"], 2)


    def test_resource_without_external_id_is_skipped_and_counted(self):
        descendants = [
            {
                "resourceInternalId": 999,
                "parentResourceId": "02",
                "resourceType": "GR",
                "status": "active",
                "name": "Sem identificador externo",
            },
            resource("A", "02"),
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertEqual(snapshot["descendants_total"], 2)
        self.assertEqual(snapshot["resources_total"], 2)
        self.assertEqual(snapshot["skipped_without_resource_id"], 1)
        self.assertEqual(set(rows), {"02", "A"})

    def test_alternate_resource_id_key_is_accepted_for_compatibility(self):
        descendants = [
            {
                "id": "ALT1",
                "parentResourceId": "02",
                "resourceType": "BK",
                "status": "active",
                "name": "Compatível",
            }
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertIn("ALT1", rows)
        self.assertEqual(snapshot["skipped_without_resource_id"], 0)


    def test_cadastral_inactive_with_broken_legacy_chain_is_ignored(self):
        descendants = [
            resource("4457", "LEGACY_PARENT", kind="TCV", status="inactive"),
            resource("TEC1", "02", kind="TCV", status="active"),
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertNotIn("4457", rows)
        self.assertIn("TEC1", rows)
        self.assertEqual(snapshot["skipped_inactive_status"], 1)

    def test_active_descendant_under_inactive_ancestor_is_pruned(self):
        descendants = [
            resource("OLD_BUCKET", "LEGACY", kind="BK", status="inactive"),
            resource("TEC1", "OLD_BUCKET", kind="TCV", status="active"),
            resource("TEC2", "02", kind="TCV", status="active"),
        ]
        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=FakeOFSClient(descendants=descendants),
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertNotIn("OLD_BUCKET", rows)
        self.assertNotIn("TEC1", rows)
        self.assertIn("TEC2", rows)
        self.assertEqual(snapshot["skipped_inactive_status"], 1)
        self.assertEqual(snapshot["skipped_under_inactive_ancestor"], 1)

    def test_active_resource_becoming_cadastral_inactive_is_removed(self):
        repo = MemoryRepository()
        service.sync_resource_hierarchy(
            client=FakeOFSClient(descendants=[resource("TEC1", "02", kind="TCV")]),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
        )
        self.assertIn("TEC1", repo.rows)

        result = service.sync_resource_hierarchy(
            client=FakeOFSClient(
                descendants=[resource("TEC1", "BROKEN", kind="TCV", status="inactive")]
            ),
            root_resource_id="02",
            repository=repo,
            use_lock=False,
        )
        self.assertNotIn("TEC1", repo.rows)
        self.assertEqual(result["skipped_inactive_status"], 1)
        self.assertEqual(result["removed_total"], 1)

    def test_inactive_group_with_stale_descendants_status_is_rechecked_and_prunes_branch(self):
        descendants = [
            # /descendants pode trazer status inconsistente em recurso legado.
            resource("BK_CARANDAI", None, kind="GR", status="active"),
            resource("BK_CHILD", "BK_CARANDAI", kind="BK", status="active"),
            resource("TEC1", "BK_CHILD", kind="TCV", status="active"),
            resource("TEC_OK", "02", kind="TCV", status="active"),
        ]
        client = FakeOFSClient(
            descendants=descendants,
            resource_details={
                "BK_CARANDAI": resource(
                    "BK_CARANDAI",
                    None,
                    kind="GR",
                    status="inactive",
                )
            },
        )

        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=client,
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertNotIn("BK_CARANDAI", rows)
        self.assertNotIn("BK_CHILD", rows)
        self.assertNotIn("TEC1", rows)
        self.assertIn("TEC_OK", rows)
        self.assertEqual(snapshot["status_rechecks_total"], 1)
        self.assertEqual(snapshot["inactive_confirmed_by_recheck"], 1)
        self.assertEqual(snapshot["skipped_inactive_status"], 1)
        self.assertEqual(snapshot["skipped_under_inactive_ancestor"], 2)

    def test_missing_parent_confirmed_inactive_prunes_entire_branch(self):
        descendants = [
            resource("BK_CHILD", "OLD_GROUP", kind="BK", status="active"),
            resource("TEC1", "BK_CHILD", kind="TCV", status="active"),
            resource("TEC_OK", "02", kind="TCV", status="active"),
        ]
        client = FakeOFSClient(
            descendants=descendants,
            resource_details={
                "BK_CHILD": resource("BK_CHILD", "OLD_GROUP", kind="BK", status="active"),
                "OLD_GROUP": resource("OLD_GROUP", None, kind="GR", status="inactive"),
            },
        )

        snapshot = service.fetch_resource_hierarchy_snapshot(
            client=client,
            root_resource_id="02",
        )
        rows = {row["resource_id"]: row for row in snapshot["rows"]}

        self.assertNotIn("BK_CHILD", rows)
        self.assertNotIn("TEC1", rows)
        self.assertIn("TEC_OK", rows)
        self.assertEqual(snapshot["status_rechecks_total"], 2)
        self.assertEqual(snapshot["inactive_confirmed_by_recheck"], 0)
        self.assertEqual(snapshot["skipped_under_inactive_ancestor"], 2)

    def test_broken_parent_chain_is_rejected(self):
        with self.assertRaises(service.HierarchySyncError):
            service.fetch_resource_hierarchy_snapshot(
                client=FakeOFSClient(descendants=[resource("TEC1", "MISSING", kind="TCV")]),
                root_resource_id="02",
            )


if __name__ == "__main__":
    unittest.main()
