import importlib.util
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT_DIR / "tools" / "ofs_operational_api_probe.py"
SPEC = importlib.util.spec_from_file_location("ofs_operational_api_probe", MODULE_PATH)
probe = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)


class _FakeResponse:
    def __init__(self, payload=None, status_code=200, url="https://ofs.example/rest/ofscCore/v1/test"):
        self._payload = payload
        self.status_code = status_code
        self.url = url
        self.ok = 200 <= status_code < 300
        self.content = b"" if payload is None else b"{}"
        self.text = ""

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, preexisting_subscription):
        self.auth = None
        self.preexisting_subscription = preexisting_subscription
        self.calls = []
        self.subscription_id = "sub-existing" if preexisting_subscription else "sub-new"

    def request(self, method, url, params=None, json=None, timeout=None, headers=None):
        self.calls.append((method, url, params, json))
        path = url.split("/rest/ofscCore/v1", 1)[-1]

        if method == "GET" and path.startswith("/resources/TEC1?"):
            return _FakeResponse({"resourceId": "TEC1", "resourceType": "TCV", "status": "active", "timeZone": "America/Sao_Paulo"}, url=url)
        if method == "GET" and path == "/resources/TEC1":
            return _FakeResponse({"resourceId": "TEC1", "resourceType": "TCV", "status": "active", "timeZone": "America/Sao_Paulo"}, url=url)
        if method == "GET" and "/resources/TEC1/workSchedules/calendarView" in path:
            return _FakeResponse({"2026-08-26": {"regular": {"recordType": "working", "workTimeStart": "08:00", "workTimeEnd": "18:00"}}}, url=url)
        if method == "GET" and path == "/calendars":
            return _FakeResponse({"items": [{"resourceId": "TEC1", "date": "2026-08-26"}]}, url=url)
        if method == "GET" and "/resources/TEC1/routes/2026-08-26" in path:
            return _FakeResponse({"totalResults": 0, "items": [], "routeStartTime": None, "routeEndTime": None}, url=url)
        if method == "GET" and path == "/activities":
            return _FakeResponse({"totalResults": 0, "items": [], "hasMore": False}, url=url)
        if method == "GET" and path == "/metadata-catalog/events":
            return _FakeResponse({"definitions": {"eventType": {"enum": list(probe.REQUESTED_EVENTS)}}}, url=url)
        if method == "GET" and path == "/events/subscriptions":
            items = [{"subscriptionId": "sub-existing"}] if self.preexisting_subscription else []
            return _FakeResponse({"items": items}, url=url)
        if method == "POST" and path == "/events/subscriptions":
            return _FakeResponse({"subscriptionId": self.subscription_id, "nextPage": "cursor-1"}, url=url)
        if method == "GET" and path == "/events":
            return _FakeResponse({"items": [], "nextPage": "cursor-1", "found": False}, url=url)
        if method == "DELETE" and path == f"/events/subscriptions/{self.subscription_id}":
            return _FakeResponse(None, status_code=204, url=url)
        raise AssertionError(f"unexpected request: {method} {path}")


class _FakeClient:
    username = "configured"
    password = "configured"
    auth = ("configured", "configured")
    base_url = "https://ofs.example/rest/ofscCore/v1"


def _probe_args():
    return SimpleNamespace(
        root="02",
        date="2026-08-26",
        resource_id="TEC1",
        timeout=5,
        events_poll_seconds=60,
        calendar_probe_limit=20,
        max_route_pages=2,
        activity_probe_limit=20,
        event_polls=1,
        event_limit=100,
        event_poll_interval=0,
        keep_subscription=False,
    )


class OperationalApiProbeTests(unittest.TestCase):
    def test_subscription_payload_separates_route_and_activity_configs(self):
        payload = probe.build_subscription_payload("teste")
        configs = payload["subscriptionConfig"]

        self.assertEqual(configs[0], {"events": list(probe.ROUTE_EVENTS)})
        self.assertEqual(configs[1]["events"], list(probe.ACTIVITY_EVENTS))
        self.assertEqual(configs[1]["fields"], list(probe.ACTIVITY_FIELDS))
        self.assertNotIn("fields", configs[0])

    def test_metadata_event_extraction_returns_requested_supported_events(self):
        payload = {
            "definitions": {
                "eventType": {
                    "enum": [
                        "activityStarted",
                        "routeActivated",
                        "inventoryInstalled",
                        "routeDeactivated",
                    ]
                }
            }
        }
        result = probe.extract_supported_events_from_metadata(payload)
        self.assertEqual(
            result,
            ["activityStarted", "routeActivated", "routeDeactivated"],
        )

    def test_route_event_observations_capture_calendar_fields_without_credentials(self):
        items = [
            {
                "eventType": "routeActivated",
                "time": "2026-08-26 11:00:00",
                "routeChanges": {
                    "resourceId": "TEC1",
                    "date": "2026-08-26",
                    "calendarTimeFrom": "08:00",
                    "calendarTimeTo": "18:00",
                    "timeZone": "America/Sao_Paulo",
                    "activated": "2026-08-26 11:00:00",
                },
            }
        ]
        result = probe.extract_route_event_observations(items)

        self.assertEqual(result["event_type_counts"], {"routeActivated": 1})
        self.assertIn("calendarTimeFrom", result["route_event_fields_seen"])
        self.assertEqual(result["calendarTimeFrom_observed"], ["08:00"])
        self.assertEqual(result["calendarTimeTo_observed"], ["18:00"])
        self.assertEqual(result["timezone_observed"], ["America/Sao_Paulo"])

    def test_activity_event_samples_capture_move_destination_fields(self):
        items = [
            {
                "eventType": "activityMoved",
                "activityDetails": {
                    "activityId": 10,
                    "resourceId": "OLD",
                    "date": "2026-08-26",
                    "status": "pending",
                },
                "activityChanges": {
                    "resourceId": "NEW",
                    "date": "2026-08-27",
                },
            }
        ]
        result = probe.extract_route_event_observations(items)
        sample = result["activity_event_samples"][0]

        self.assertEqual(sample["activityDetails"]["resourceId"], "OLD")
        self.assertEqual(sample["activityChanges"]["resourceId"], "NEW")
        self.assertEqual(sample["activityChanges"]["date"], "2026-08-27")

    def test_activity_summary_counts_statuses_and_fields(self):
        result = probe.summarize_activity_items(
            [
                {"activityId": 1, "status": "pending", "resourceId": "T1"},
                {"activityId": 2, "status": "started", "resourceId": "T1"},
                {"activityId": 3, "status": "started", "resourceId": "T1"},
            ]
        )
        self.assertEqual(result["status_counts"], {"pending": 1, "started": 2})
        self.assertIn("activityId", result["fields_seen"])
        self.assertIn("resourceId", result["fields_seen"])

    def test_calendar_summary_extracts_working_window(self):
        result = probe.summarize_calendar_payload(
            {
                "2026-08-26": {
                    "regular": {
                        "recordType": "working",
                        "workTimeStart": "08:00",
                        "workTimeEnd": "18:00",
                    }
                },
                "links": [],
            },
            "2026-08-26",
        )
        self.assertTrue(result["present"])
        self.assertEqual(result["regular"]["workTimeStart"], "08:00")
        self.assertEqual(result["regular"]["workTimeEnd"], "18:00")

    def test_performance_estimate_exposes_route_polling_explosion(self):
        result = probe.estimate_api_volume(2046, 1333, events_poll_seconds=60)

        self.assertEqual(result["calendar_baseline_calls_upper_bound_one_day"], 21)
        self.assertEqual(result["route_baseline_minimum_calls_if_all_technicians"], 1333)
        self.assertEqual(
            result["route_polling_calls_per_day_if_each_technician_every_minute"],
            1333 * 1440,
        )
        self.assertEqual(result["events_get_calls_per_day_at_poll_interval"], 1440)

    def test_sanitizer_redacts_sensitive_keys_and_inline_values(self):
        sanitized = probe._sanitize_json(
            {
                "Authorization": "Basic abc",
                "client_secret": "secret-value",
                "detail": "password=hunter2 failed",
                "safe": "ok",
            }
        )
        self.assertEqual(sanitized["Authorization"], "<redacted>")
        self.assertEqual(sanitized["client_secret"], "<redacted>")
        self.assertNotIn("hunter2", sanitized["detail"])
        self.assertEqual(sanitized["safe"], "ok")

    def test_subscription_fingerprint_never_returns_full_long_id(self):
        subscription_id = "1234567890abcdefghijklmnop"
        result = probe.subscription_fingerprint(subscription_id)
        self.assertNotIn(subscription_id, result)
        self.assertIn("suffix=", result)

    def test_extract_subscription_ids(self):
        payload = {
            "items": [
                {"subscriptionId": "one"},
                {"subscriptionId": "two"},
                {"subscriptionId": ""},
            ]
        }
        self.assertEqual(probe.extract_subscription_ids(payload), {"one", "two"})

    def test_run_probe_deletes_only_subscription_proven_new(self):
        fake_session = _FakeSession(preexisting_subscription=False)
        hierarchy = {
            "accessible": True,
            "root_resource_id": "02",
            "hierarchy_total": 2046,
            "technician_total": 1333,
            "max_depth": 7,
            "resource_type_counts": {"TCV": 709, "TCP": 591, "TCW": 33},
            "sample_technician": {"resource_id": "TEC1"},
        }

        with patch.object(probe, "load_hierarchy_context", return_value=hierarchy), \
             patch.object(probe, "OFSClient", return_value=_FakeClient()), \
             patch.object(probe.requests, "Session", return_value=fake_session):
            report = probe.run_probe(_probe_args())

        self.assertTrue(report["subscription_cleanup"]["deleted"])
        self.assertTrue(any(call[0] == "DELETE" for call in fake_session.calls))

    def test_run_probe_never_deletes_reused_preexisting_subscription(self):
        fake_session = _FakeSession(preexisting_subscription=True)
        hierarchy = {
            "accessible": True,
            "root_resource_id": "02",
            "hierarchy_total": 2046,
            "technician_total": 1333,
            "max_depth": 7,
            "resource_type_counts": {"TCV": 709, "TCP": 591, "TCW": 33},
            "sample_technician": {"resource_id": "TEC1"},
        }

        with patch.object(probe, "load_hierarchy_context", return_value=hierarchy), \
             patch.object(probe, "OFSClient", return_value=_FakeClient()), \
             patch.object(probe.requests, "Session", return_value=fake_session):
            report = probe.run_probe(_probe_args())

        self.assertFalse(report["subscription_cleanup"]["attempted"])
        self.assertIn("já existia", report["subscription_cleanup"]["reason"])
        self.assertFalse(any(call[0] == "DELETE" for call in fake_session.calls))


if __name__ == "__main__":
    unittest.main()
