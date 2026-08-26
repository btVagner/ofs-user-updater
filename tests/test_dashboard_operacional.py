import json
import sys
import types
import unittest
from collections import Counter, defaultdict
from datetime import datetime
from unittest.mock import Mock, patch

# O serviço possui imports de banco no módulo. Estes testes são deliberadamente
# unitários e não devem abrir conexão real; injetamos somente a função esperada
# antes do import da árvore de serviços.
fake_connection_module = types.ModuleType("database.connection")
fake_connection_module.get_connection = lambda: (_ for _ in ()).throw(
    AssertionError("conexão real não deve ser usada neste teste")
)
sys.modules["database.connection"] = fake_connection_module

from services import dashboard_operacional_service as service  # noqa: E402


FIXED_NOW = datetime(2026, 8, 25, 14, 30, 0)
TODAY = "2026-08-25"
LAST_WEEK = "2026-08-18"
LAST_7 = [
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
    "2026-08-22",
    "2026-08-23",
    "2026-08-24",
    "2026-08-25",
]

ACTIVITY_MAPS = {
    "labels": {
        "INST": "Instalação",
        "SUP": "Suporte padrão",
        "SUP_QUA": "Suporte qualidade",
        "SUP_REP": "Suporte reparo",
        "INF_COR": "Infra corretiva",
        "MAN_PRE": "Manutenção preventiva",
    },
    "b2c_codes": {"INST", "SUP", "SUP_QUA", "SUP_REP"},
    "redes_codes": {"INF_COR", "MAN_PRE"},
}


def row(day, status, activity_type, city, end_time, appt, rating="", category="", subcategory=""):
    return {
        "date": day,
        "status": status,
        "activityType": activity_type,
        "city": city,
        "endTime": f"{day} {end_time}:00" if end_time else "",
        "apptNumber": appt,
        "XA_AV_CLI": rating,
        "XA_AV_CLI_CAT": category,
        "XA_AV_CLI_SUB_CAT": subcategory,
        "XA_AV_CLI_CON": "1" if rating else "",
    }


ROWS = [
    # Mesmo dia da semana anterior: apenas atividades concluídas até 14:30 devem
    # participar dos KPIs comparativos.
    row(LAST_WEEK, "completed", "INST", "Porto Alegre", "10:00", "LW-1"),
    row(LAST_WEEK, "completed", "SUP", "Canoas", "15:10", "LW-2"),
    row(LAST_WEEK, "completed", "INF_COR", "Osório", "09:00", "LW-3"),
    row(LAST_WEEK, "notdone", "MAN_PRE", "Osório", "11:00", "LW-4"),
    # Últimos 7 dias: mistura de concluídas/improdutivas antes/depois de 14:30.
    row("2026-08-19", "completed", "INST", "Canoas", "12:00", "D1-1"),
    row("2026-08-19", "notdone", "INF_COR", "Osório", "13:00", "D1-2"),
    row("2026-08-20", "completed", "SUP_QUA", "Gravataí", "16:00", "D2-1"),
    row("2026-08-21", "notdone", "SUP_REP", "Gravataí", "09:15", "D3-1"),
    row("2026-08-22", "completed", "MAN_PRE", "Viamão", "08:30", "D4-1"),
    row("2026-08-23", "completed", "INF_COR", "Osório", "14:30", "D5-1"),
    row("2026-08-24", "notdone", "INST", "Canoas", "17:00", "D6-1"),
    # Hoje: cobre todos os status obrigatórios e ambos os grupos.
    row(TODAY, "completed", "INST", "Canoas", "10:00", "T-1", "5", "Atendimento", "Cordialidade"),
    row(TODAY, "notdone", "SUP", "Canoas", "11:00", "T-2", "2", "Prazo", "Atraso"),
    row(TODAY, "pending", "SUP_QUA", "Gravataí", "", "T-3"),
    row(TODAY, "started", "SUP_REP", "Gravataí", "", "T-4"),
    row(TODAY, "suspended", "INF_COR", "Osório", "", "T-5"),
    row(TODAY, "cancelled", "MAN_PRE", "Osório", "", "T-6"),
    row(TODAY, "enroute", "INF_COR", "Tramandaí", "", "T-7"),
    row(TODAY, "completed", "INF_COR", "Osório", "16:20", "T-8", "3", "Execução", "Retrabalho"),
    row(TODAY, "notdone", "MAN_PRE", "Tramandaí", "12:20", "T-9", "4", "Atendimento", "Cordialidade"),
]


def project_legacy_rows(rows):
    projected = []
    b2c = ACTIVITY_MAPS["b2c_codes"]
    redes = ACTIVITY_MAPS["redes_codes"]
    labels = ACTIVITY_MAPS["labels"]
    for item in rows:
        activity_type = item["activityType"]
        projected.append({
            "date": item["date"],
            "status": item["status"],
            "activityType": activity_type,
            "activityTypeFilterCode": service._dashboard_type_filter_code(activity_type),
            "activityTypeLabel": service._dashboard_type_label(activity_type, labels),
            "city": item["city"],
            "endTime": item["endTime"],
            "group": "redes" if activity_type in redes else "b2c" if activity_type in b2c else "outros",
        })
    return projected


def before_metrics(projected_rows, selected_types):
    filtered = [
        item for item in projected_rows
        if item["activityTypeFilterCode"] in selected_types
    ]

    status_today = Counter(
        item["status"] for item in filtered if item["date"] == TODAY
    )

    evolution = {day: {"completed": 0, "notdone": 0} for day in LAST_7}
    evolution_until = {day: {"completed": 0, "notdone": 0} for day in LAST_7}
    for item in filtered:
        if item["date"] not in evolution:
            continue
        if item["status"] in {"completed", "notdone"}:
            evolution[item["date"]][item["status"]] += 1
            parsed = service._parse_ofs_datetime(item["endTime"])
            if parsed and parsed.time() <= FIXED_NOW.time():
                evolution_until[item["date"]][item["status"]] += 1

    cities = {"b2c": defaultdict(lambda: {"total": 0, "completed": 0, "notdone": 0}),
              "redes": defaultdict(lambda: {"total": 0, "completed": 0, "notdone": 0})}
    for item in filtered:
        if item["date"] != TODAY or item["group"] not in cities:
            continue
        values = cities[item["group"]][item["city"]]
        values["total"] += 1
        if item["status"] == "completed":
            values["completed"] += 1
        if item["status"] == "notdone":
            values["notdone"] += 1

    return {
        "status_today": dict(status_today),
        "evolution": evolution,
        "evolution_until": evolution_until,
        "cities": {
            group: {city: dict(values) for city, values in group_rows.items()}
            for group, group_rows in cities.items()
        },
    }


def after_metrics(payload, selected_types):
    type_model = payload["filter_read_model"]["types"]
    option_by_code = {item["code"]: item for item in payload["activity_options"]}

    status_today = Counter()
    evolution = {day: {"completed": 0, "notdone": 0} for day in LAST_7}
    evolution_until = {day: {"completed": 0, "notdone": 0} for day in LAST_7}
    cities = {"b2c": defaultdict(lambda: {"total": 0, "completed": 0, "notdone": 0}),
              "redes": defaultdict(lambda: {"total": 0, "completed": 0, "notdone": 0})}

    for code in selected_types:
        metrics = type_model.get(code) or {}
        status_today.update({key: int(value) for key, value in (metrics.get("status_today") or {}).items()})

        for day in LAST_7:
            for target, source_name in ((evolution, "evolution"), (evolution_until, "evolution_until")):
                values = (metrics.get(source_name) or {}).get(day) or {}
                target[day]["completed"] += int(values.get("completed") or 0)
                target[day]["notdone"] += int(values.get("notdone") or 0)

        group = (option_by_code.get(code) or {}).get("group")
        if group not in cities:
            continue
        for item in metrics.get("cities_today") or []:
            values = cities[group][item["city"]]
            values["total"] += int(item.get("total") or 0)
            values["completed"] += int(item.get("completed") or 0)
            values["notdone"] += int(item.get("notdone") or 0)

    return {
        "status_today": dict(status_today),
        "evolution": evolution,
        "evolution_until": evolution_until,
        "cities": {
            group: {city: dict(values) for city, values in group_rows.items()}
            for group, group_rows in cities.items()
        },
    }


class DashboardReadModelTests(unittest.TestCase):
    def build_payload(self):
        with patch.object(service, "_now", return_value=FIXED_NOW):
            return service._build_payload(ROWS, ACTIVITY_MAPS)

    def test_new_payload_has_no_raw_dashboard_rows(self):
        payload = self.build_payload()
        self.assertNotIn("dashboard_rows", payload)
        self.assertIn("filter_read_model", payload)
        self.assertGreater(len(payload["filter_read_model"]["types"]), 0)

    def test_filters_status_evolution_temporal_and_cities_match_legacy_behavior(self):
        payload = self.build_payload()
        projected = project_legacy_rows(ROWS)
        selections = [
            {"INST", "SUPORTE", "INF_COR", "MAN_PRE"},
            {"INST", "SUPORTE"},
            {"INF_COR", "MAN_PRE"},
            {"SUPORTE"},
            {"INF_COR"},
            set(),
        ]

        for selected in selections:
            with self.subTest(selected=sorted(selected)):
                self.assertEqual(before_metrics(projected, selected), after_metrics(payload, selected))

    def test_all_required_today_statuses_survive_aggregation(self):
        payload = self.build_payload()
        selected = {item["code"] for item in payload["activity_options"]}
        actual = after_metrics(payload, selected)["status_today"]
        for status in ("completed", "notdone", "pending", "started", "suspended", "cancelled", "enroute"):
            self.assertGreater(actual.get(status, 0), 0, status)

    def test_b2c_redes_types_kpis_and_customer_thermometer_are_preserved(self):
        payload = self.build_payload()

        b2c_types = {item["code"]: item["total"] for item in payload["b2c_by_type_today"]}
        redes_types = {item["code"]: item["total"] for item in payload["redes_by_type_today"]}
        self.assertEqual(b2c_types["SUPORTE"], 3)
        self.assertEqual(b2c_types["INST"], 1)
        self.assertEqual(redes_types["INF_COR"], 3)
        self.assertEqual(redes_types["MAN_PRE"], 2)

        kpis = payload["kpis"]
        self.assertEqual(kpis["b2c_completed_today"], 1)
        self.assertEqual(kpis["redes_completed_today"], 1)
        self.assertEqual(kpis["b2c_completed_last_week_same_day"], 1)
        self.assertEqual(kpis["redes_completed_last_week_same_day"], 1)
        self.assertEqual(kpis["b2c_notdone_today"], 1)
        self.assertEqual(kpis["redes_notdone_today"], 1)

        thermometer = payload["customer_thermometer"]
        self.assertEqual(thermometer["summary"]["total"], 4)
        self.assertEqual(thermometer["summary"]["critical"], 2)
        self.assertEqual(len(thermometer["critical_rows"]), 2)

    def test_legacy_snapshot_is_compacted_before_browser_serialization(self):
        payload = self.build_payload()
        legacy_rows = project_legacy_rows(ROWS)
        legacy_payload = dict(payload)
        legacy_payload.pop("filter_read_model", None)
        legacy_payload["dashboard_rows"] = legacy_rows

        compact = service._prepare_payload_for_browser(legacy_payload)
        self.assertNotIn("dashboard_rows", compact)
        self.assertIn("filter_read_model", compact)
        self.assertEqual(
            before_metrics(legacy_rows, {"SUPORTE", "INF_COR"}),
            after_metrics(compact, {"SUPORTE", "INF_COR"}),
        )

    def test_compact_payload_is_materially_smaller_than_legacy_projection(self):
        payload = self.build_payload()
        legacy_payload = dict(payload)
        legacy_payload["dashboard_rows"] = project_legacy_rows(ROWS) * 1000
        legacy_payload.pop("filter_read_model", None)

        compact_payload = service._prepare_payload_for_browser(legacy_payload)
        legacy_bytes = len(json.dumps(legacy_payload, ensure_ascii=False).encode("utf-8"))
        compact_bytes = len(json.dumps(compact_payload, ensure_ascii=False).encode("utf-8"))
        self.assertLess(compact_bytes, legacy_bytes * 0.1)


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        return dict(self.row) if self.row is not None else None

    def close(self):
        pass


class FakeConnection:
    def __init__(self, row):
        self.cursor_instance = FakeCursor(row)

    def cursor(self, dictionary=False):
        return self.cursor_instance

    def close(self):
        pass


class DashboardSnapshotQueryTests(unittest.TestCase):
    def metadata_row(self):
        return {
            "snapshot_key": service.SNAPSHOT_KEY,
            "status": "completed",
            "error_text": None,
            "updated_at": FIXED_NOW,
            "expires_at": FIXED_NOW,
            "started_at": None,
            "finished_at": FIXED_NOW,
            "progress_percent": 100,
            "progress_message": "ok",
            "progress_updated_at": FIXED_NOW,
            "has_payload": 1,
        }

    def test_status_path_does_not_select_or_deserialize_payload(self):
        connection = FakeConnection(self.metadata_row())
        with patch.object(service, "get_connection", return_value=connection), patch.object(
            service, "_json_loads", side_effect=AssertionError("status não pode desserializar payload")
        ):
            status = service.get_dashboard_snapshot_status()

        self.assertTrue(status["has_payload"])
        self.assertEqual(status["status"], "completed")
        sql = connection.cursor_instance.queries[0][0]
        select_clause = sql.upper().split(" FROM ")[0]
        self.assertIn("PAYLOAD_JSON IS NOT NULL AS HAS_PAYLOAD", select_clause)
        self.assertNotIn("PAYLOAD_JSON,", select_clause)
        self.assertNotIn("JSON_EXTRACT", select_clause)
        self.assertNotIn("JSON_LENGTH", select_clause)

    def test_full_snapshot_read_has_no_ddl_in_hot_path(self):
        row = self.metadata_row()
        row["payload_json"] = "{}"
        connection = FakeConnection(row)
        with patch.object(service, "get_connection", return_value=connection):
            snapshot = service._load_snapshot()

        self.assertEqual(snapshot["payload"], {})
        all_sql = "\n".join(sql for sql, _params in connection.cursor_instance.queries).upper()
        self.assertNotIn("CREATE TABLE", all_sql)
        self.assertNotIn("ALTER TABLE", all_sql)

    def test_valid_legacy_snapshot_starts_compact_refresh_without_blocking_home(self):
        legacy_payload = {
            "periods": {
                "today": TODAY,
                "last_7_days_from": "2026-08-19",
                "last_7_days_to": TODAY,
                "comparison_until_time": "14:30",
            },
            "dashboard_rows": project_legacy_rows(ROWS[:3]),
        }
        snapshot = {
            "status": "completed",
            "payload": legacy_payload,
            "updated_at": FIXED_NOW,
            "expires_at": datetime(2026, 8, 25, 14, 40, 0),
            "started_at": None,
            "finished_at": FIXED_NOW,
        }
        starter = Mock()

        with patch.object(service, "_load_snapshot", return_value=snapshot), patch.object(
            service, "_try_mark_running", return_value=True
        ), patch.object(service, "_start_background_refresh", starter), patch.object(
            service, "_now", return_value=FIXED_NOW
        ):
            serialized = service.get_or_start_dashboard_snapshot()

        starter.assert_called_once_with()
        self.assertEqual(serialized["status"], "running")
        self.assertTrue(serialized["has_payload"])
        self.assertNotIn("dashboard_rows", serialized["payload"])
        self.assertIn("filter_read_model", serialized["payload"])


if __name__ == "__main__":
    unittest.main()
