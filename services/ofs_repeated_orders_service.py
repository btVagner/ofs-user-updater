"""Shared seven-day analysis. Web reads/enqueues only; a separate worker collects."""
from __future__ import annotations

import json
import unicodedata
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from math import ceil

from services.ofs_operational_monitor_service import NON_OS_RECORD_TYPES, NON_OS_TYPES, _is_withdrawal
from services.ofs_technician_operational_service import operational_today, sanitize_operational_error

VIEW_PERMISSION = "ofs.os_repetidas.visualizar"
REFRESH_PERMISSION = "ofs.os_repetidas.atualizar"
LOCK_NAME = "ofs_repeated_orders_worker"
FIELDS = ("activityId", "customerNumber", "apptNumber", "XA_ORG_SYS", "activityType",
          "date", "status", "resourceId", "recordType", "timeSlot", "stateProvince")
COOLDOWN = 600
ROOT_RESOURCE_ID = "02"
MAX_RECONCILIATIONS = 100
ALLOWED_STATUSES = frozenset({"pending", "enroute", "started"})


def remaining_seconds(gate):
    anchors = [gate[key] for key in ("last_requested_at", "last_started_at") if gate.get(key)]
    return max(0, ceil(COOLDOWN - (gate["db_now"] - max(anchors)).total_seconds())) if anchors else 0


class RepeatedOrdersError(RuntimeError):
    pass


class RefreshRejected(RepeatedOrdersError):
    def __init__(self, code, seconds=0):
        super().__init__("Já existe uma atualização pendente ou em execução." if code == "BUSY"
                         else "Aguarde o intervalo global de 10 minutos.")
        self.code = code
        self.seconds = seconds


def text(value):
    return str(value if value is not None else "").strip()


def state_key(value):
    return " ".join("".join(char for char in unicodedata.normalize("NFKD", text(value).upper())
                            if not unicodedata.combining(char)).split())


STATE_NAMES = {
    "AC": "ACRE", "AL": "ALAGOAS", "AP": "AMAPA", "AM": "AMAZONAS", "BA": "BAHIA",
    "CE": "CEARA", "DF": "DISTRITO FEDERAL", "ES": "ESPIRITO SANTO", "GO": "GOIAS",
    "MA": "MARANHAO", "MT": "MATO GROSSO", "MS": "MATO GROSSO DO SUL", "MG": "MINAS GERAIS",
    "PA": "PARA", "PB": "PARAIBA", "PR": "PARANA", "PE": "PERNAMBUCO", "PI": "PIAUI",
    "RJ": "RIO DE JANEIRO", "RN": "RIO GRANDE DO NORTE", "RS": "RIO GRANDE DO SUL",
    "RO": "RONDONIA", "RR": "RORAIMA", "SC": "SANTA CATARINA", "SP": "SAO PAULO",
    "SE": "SERGIPE", "TO": "TOCANTINS",
}
STATE_CODES = {name: code for code, name in STATE_NAMES.items()}


def normalize_state(value):
    value = state_key(value)
    return value if value in STATE_NAMES else STATE_CODES.get(value, "")


def analyze(activities, start: date, allowed_resources=None):
    """Fail on conflicting IDs; exclude incomplete orders with counted diagnostics."""
    end = start + timedelta(days=6)
    diagnostics = {key: 0 for key in ("missing_contract", "missing_appt", "missing_type",
                  "invalid_date", "outside_window", "non_os", "withdrawal", "outside_hierarchy",
                  "missing_resource", "excluded_status", "duplicate_activity", "duplicate_os")}
    diagnostic_samples = {key: [] for key in ("missing_contract", "missing_appt", "missing_type", "invalid_date",
                                              "outside_hierarchy", "missing_resource")}
    def record_issue(reason, row):
        diagnostics[reason] += 1
        if len(diagnostic_samples[reason]) < 20:
            diagnostic_samples[reason].append({key: row[key] for key in
                                              ("activityId", "customerNumber", "apptNumber", "date", "resourceId")})
    contracts, seen = {}, {}
    for raw in activities:
        row = {key: text(raw.get(key)) for key in FIELDS}
        if _is_withdrawal({"activity_type": row["activityType"]}):
            diagnostics["withdrawal"] += 1
            continue
        if row["status"].lower() not in ALLOWED_STATUSES:
            diagnostics["excluded_status"] += 1
            continue
        if allowed_resources is not None:
            if not row["resourceId"]:
                record_issue("missing_resource", row)
                continue
            if row["resourceId"] not in allowed_resources:
                record_issue("outside_hierarchy", row)
                continue
        try:
            activity_date = date.fromisoformat(row["date"])
        except ValueError:
            record_issue("invalid_date", row)
            continue
        if not start <= activity_date <= end:
            diagnostics["outside_window"] += 1
            continue
        if row["activityType"].upper() in NON_OS_TYPES or row["recordType"].lower() in NON_OS_RECORD_TYPES:
            diagnostics["non_os"] += 1
            continue
        if not row["activityId"]:
            raise RepeatedOrdersError("Resposta OFS contém OS sem activityId; resultado não publicado.")
        if row["activityId"] in seen:
            if seen[row["activityId"]] != row:
                raise RepeatedOrdersError("Resposta OFS contém activityId conflitante; tente nova coleta.")
            diagnostics["duplicate_activity"] += 1
            continue
        seen[row["activityId"]] = dict(row)
        row["regional"] = normalize_state(row["stateProvince"])
        missing = False
        for field, counter in (("customerNumber", "missing_contract"), ("apptNumber", "missing_appt"),
                               ("activityType", "missing_type")):
            if not row[field]:
                record_issue(counter, row)
                missing = True
        if missing:
            continue
        origin = row["XA_ORG_SYS"].upper()
        appt = row["apptNumber"]
        prefix = appt.split("-", 1)[0] if origin == "NG" else appt
        if not prefix:
            record_issue("missing_appt", row)
            continue
        key = (origin, prefix)
        orders = contracts.setdefault(row["customerNumber"], {})
        if key not in orders:
            orders[key] = {"key": prefix, "origin": origin or "Não informada", "types": set(), "activities": []}
        elif origin != "NG" or any(a["apptNumber"] == appt for a in orders[key]["activities"]):
            diagnostics["duplicate_os"] += 1
        orders[key]["types"].add(row["activityType"].upper())
        orders[key]["activities"].append(row)
    result = []
    summary = {"yellow": 0, "red": 0, "purple": 0}
    for contract, orders in contracts.items():
        if len(orders) < 2:
            continue
        types = set().union(*(order["types"] for order in orders.values()))
        severity = "purple" if len(orders) >= 3 else "red" if len(types) == 1 else "yellow"
        summary[severity] += 1
        details = []
        for key in sorted(orders):
            order = orders[key]
            order["types"] = sorted(order["types"])
            order["activities"].sort(key=lambda a: (a["date"], a["apptNumber"], a["activityId"]))
            details.append(order)
        result.append({"contract": contract, "severity": severity, "count": len(orders),
                       "types": sorted(types), "orders": details})
    rank = {"purple": 0, "red": 1, "yellow": 2}
    result.sort(key=lambda c: (rank[c["severity"]], c["contract"]))
    return {"date_from": start.isoformat(), "date_to": end.isoformat(), "summary": summary,
            "diagnostics": diagnostics, "diagnostic_samples": diagnostic_samples,
            "allowed_statuses": sorted(ALLOWED_STATUSES), "status_policy_source": "full_collection",
            "contracts": result, "received": len(activities)}


def apply_status_policy(payload):
    """Narrow an old snapshot locally. Removing records cannot create new alerts."""
    if not payload or payload.get("allowed_statuses") == sorted(ALLOWED_STATUSES):
        return payload
    rows = [activity for contract in payload.get("contracts", [])
            for order in contract["orders"] for activity in order["activities"]]
    projected = analyze(rows, date.fromisoformat(payload["date_from"]))
    result = dict(payload)
    for key in ("contracts", "summary", "allowed_statuses"):
        result[key] = projected[key]
    result["diagnostics"] = dict(payload.get("diagnostics", {}))
    result["diagnostics"]["excluded_status_from_previous_mesh"] = projected["diagnostics"]["excluded_status"]
    result["status_policy_source"] = "previous_mesh"
    return result


def collect(api, start, root, progress, limit=1000, max_pages=10000):
    if root != ROOT_RESOURCE_ID:
        raise RepeatedOrdersError("Esta análise é restrita à hierarquia do recurso 02.")
    rows, offset, pages = [], 0, 0
    while pages < max_pages:
        payload = api._request("GET", "activities", params={
            "resources": root, "includeChildren": "all", "includeNonScheduled": "false",
            "dateFrom": start.isoformat(), "dateTo": (start + timedelta(days=6)).isoformat(),
            "fields": ",".join(FIELDS), "limit": limit, "offset": offset,
        })
        items = payload.get("items")
        more = payload.get("hasMore", False)
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items) or type(more) is not bool:
            raise RepeatedOrdersError("Página OFS inválida; resultado não publicado.")
        if more and not items:
            raise RepeatedOrdersError("OFS retornou página vazia com hasMore; resultado não publicado.")
        rows.extend(items)
        offset += len(items)
        pages += 1
        progress(pages, offset)
        if not more:
            return rows
    raise RepeatedOrdersError("Limite de páginas excedido; resultado não publicado.")


def collect_scope(api, progress=lambda: None, max_pages=1000, metrics=None):
    """Fresh descendants, read-only. Do not rely on an old local hierarchy."""
    ids, offset, total, missing_ids = {ROOT_RESOURCE_ID}, 0, None, 0
    for page in range(max_pages):
        payload = api._request("GET", "resources/02/descendants", params={
            "fields": "resourceId", "limit": 100, "offset": offset,
        })
        items, reported_total = payload.get("items"), payload.get("totalResults")
        if (not isinstance(items, list) or type(reported_total) is not int or reported_total < 0
                or any(not isinstance(item, dict) for item in items)):
            raise RepeatedOrdersError("Hierarquia 02 inválida; resultado não publicado.")
        if total is None:
            total = reported_total
        if total != reported_total:
            raise RepeatedOrdersError("Hierarquia mudou durante a paginação; tente nova coleta.")
        for item in items:
            resource_id = text(item.get("resourceId"))
            if not resource_id:
                missing_ids += 1
                continue
            if resource_id in ids and resource_id != ROOT_RESOURCE_ID:
                raise RepeatedOrdersError("Hierarquia 02 contém recurso duplicado; tente nova coleta.")
            ids.add(resource_id)
        offset += len(items)
        if metrics is not None:
            metrics.update(received=offset, missing_external_id=missing_ids, pages=page + 1)
        progress()
        if offset == total:
            return ids
        if not items or offset > total:
            raise RepeatedOrdersError("Hierarquia 02 incompleta; resultado não publicado.")
    raise RepeatedOrdersError("Limite de páginas da hierarquia excedido; resultado não publicado.")


def reconcile_duplicates(api, rows, progress=lambda: None, max_reconciliations=MAX_RECONCILIATIONS):
    """One current GET per conflicting ID; never pick an arbitrary page's value."""
    groups = {}
    for index, row in enumerate(rows):
        identifier = text(row.get("activityId"))
        groups.setdefault(identifier or ("missing", index), []).append(row)
    conflicts = []
    for identifier, variants in groups.items():
        if isinstance(identifier, str) and any(
            any(text(row.get(field)) != text(variants[0].get(field)) for field in FIELDS)
            for row in variants[1:]
        ):
            conflicts.append(identifier)
    if len(conflicts) > max_reconciliations:
        raise RepeatedOrdersError("Muitas atividades mudaram durante a coleta; resultado não publicado.")
    samples = []
    for identifier in conflicts:
        if not identifier.isascii() or not identifier.isdigit():
            raise RepeatedOrdersError("activityId inválido para reconciliação; resultado não publicado.")
        variants = groups[identifier]
        current = api._request("GET", f"activities/{identifier}")
        if text(current.get("activityId")) != identifier:
            raise RepeatedOrdersError("Reconciliação retornou activityId diferente; resultado não publicado.")
        # GET by ID returns all properties; retain only this analysis's minimal fields.
        groups[identifier] = [{field: current.get(field) for field in FIELDS}]
        if len(samples) < 20:
            changed = [field for field in FIELDS if len({text(row.get(field)) for row in variants}) > 1]
            samples.append({"activityId": identifier, "changed_fields": changed})
        progress()
    return [variants[0] for variants in groups.values()], {
        "duplicate_activity": len(rows) - len(groups), "reconciled_activity": len(conflicts),
        "reconciliation_samples": samples,
    }


def connection_factory():
    from database.connection import get_connection
    return get_connection()


@contextmanager
def transaction(factory):
    conn = factory()
    cur = conn.cursor(dictionary=True)
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def serializable(row):
    return {key: value.isoformat() + "Z" if isinstance(value, datetime) else value.isoformat()
            if isinstance(value, date) else value for key, value in row.items()}


class RepeatedOrdersRepository:
    def __init__(self, factory=None):
        self.factory = factory or connection_factory

    def request_refresh(self, actor):
        # Singleton row serializes requests from every Gunicorn process.
        with transaction(self.factory) as (_, cur):
            cur.execute("SELECT *, UTC_TIMESTAMP(6) AS db_now FROM ofs_repeated_orders_control WHERE id=1 FOR UPDATE")
            gate = cur.fetchone()
            if not gate:
                raise RepeatedOrdersError("Migração OS repetidas não inicializada.")
            cur.execute("SELECT id FROM ofs_repeated_orders_run WHERE status IN ('queued','running') LIMIT 1")
            if cur.fetchone():
                raise RefreshRejected("BUSY")
            seconds = remaining_seconds(gate)
            if seconds > 0:
                raise RefreshRejected("COOLDOWN", seconds)
            cur.execute("""INSERT INTO ofs_repeated_orders_run
                (status, requested_at, requested_by_user_id, requested_by_username)
                VALUES ('queued',UTC_TIMESTAMP(6),%s,%s)""", (actor.get("id"), actor.get("username")))
            run_id = cur.lastrowid
            cur.execute("UPDATE ofs_repeated_orders_control SET last_requested_at=UTC_TIMESTAMP(6) WHERE id=1")
            return {"id": run_id, "status": "queued"}

    def read(self):
        with transaction(self.factory) as (_, cur):
            cur.execute("SELECT *, UTC_TIMESTAMP(6) AS db_now FROM ofs_repeated_orders_control WHERE id=1")
            gate = cur.fetchone()
            if not gate:
                raise RepeatedOrdersError("Migração OS repetidas não inicializada.")
            cur.execute("SELECT * FROM ofs_repeated_orders_run ORDER BY id DESC LIMIT 1")
            run = cur.fetchone()
            cur.execute("SELECT run_id,payload_json,published_at FROM ofs_repeated_orders_snapshot WHERE id=1")
            snapshot = cur.fetchone()
            remaining = remaining_seconds(gate)
            payload = apply_status_policy(json.loads(snapshot["payload_json"])) if snapshot else None
            return {"run": serializable(run) if run else None, "payload": payload,
                    "published_at": serializable(snapshot)["published_at"] if snapshot else None,
                    "snapshot_run_id": snapshot["run_id"] if snapshot else None,
                    "remaining_seconds": remaining,
                    "stale": not payload or payload["date_from"] != operational_today().isoformat()}

    @contextmanager
    def worker_lock(self):
        # Keep this DB session for the entire execution; verify ownership before writes.
        conn = self.factory()
        cur = conn.cursor(dictionary=True)
        acquired = False
        try:
            cur.execute("SELECT GET_LOCK(%s,0) AS acquired", (LOCK_NAME,))
            acquired = cur.fetchone()["acquired"] == 1
            if not acquired:
                raise RefreshRejected("BUSY")
            yield conn, cur
        finally:
            try:
                if acquired:
                    cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
                    cur.fetchone()
            finally:
                cur.close()
                conn.close()

    def ensure_owner(self, cur):
        cur.execute("SELECT IS_USED_LOCK(%s)=CONNECTION_ID() AS owned", (LOCK_NAME,))
        if cur.fetchone()["owned"] != 1:
            raise RepeatedOrdersError("Worker perdeu o lock MySQL; resultado não publicado.")

    def claim(self, conn, cur, start):
        self.ensure_owner(cur)
        # Only a new lock owner can recover running work: never steal by heartbeat age.
        cur.execute("SELECT *, UTC_TIMESTAMP(6) AS db_now FROM ofs_repeated_orders_control WHERE id=1 FOR UPDATE")
        gate = cur.fetchone()
        if not gate:
            raise RepeatedOrdersError("Migração OS repetidas não inicializada.")
        cur.execute("""UPDATE ofs_repeated_orders_run SET status='interrupted',finished_at=UTC_TIMESTAMP(6),
            error_text='Worker interrompido; solicite nova atualização após o intervalo global.' WHERE status='running'""")
        cur.execute("SELECT id FROM ofs_repeated_orders_run WHERE status='queued' ORDER BY id LIMIT 1 FOR UPDATE")
        row = cur.fetchone()
        if gate["last_started_at"] and (gate["db_now"] - gate["last_started_at"]).total_seconds() < COOLDOWN:
            conn.commit()
            return None
        if row:
            cur.execute("""UPDATE ofs_repeated_orders_run SET status='running',started_at=UTC_TIMESTAMP(6),
                heartbeat_at=UTC_TIMESTAMP(6),date_from=%s,date_to=%s WHERE id=%s""",
                        (start, start + timedelta(days=6), row["id"]))
            cur.execute("UPDATE ofs_repeated_orders_control SET last_started_at=UTC_TIMESTAMP(6) WHERE id=1")
        conn.commit()
        return row["id"] if row else None

    def progress(self, conn, cur, run_id, pages, received):
        self.ensure_owner(cur)
        cur.execute("""UPDATE ofs_repeated_orders_run SET pages=%s,received=%s,heartbeat_at=UTC_TIMESTAMP(6)
            WHERE id=%s AND status='running'""", (pages, received, run_id))
        conn.commit()

    def finish(self, conn, cur, run_id, payload=None, error=None):
        self.ensure_owner(cur)
        try:
            cur.execute("SELECT status FROM ofs_repeated_orders_run WHERE id=%s FOR UPDATE", (run_id,))
            if cur.fetchone()["status"] != "running":
                raise RepeatedOrdersError("Execução já encerrada.")
            if payload is not None:
                cur.execute("""INSERT INTO ofs_repeated_orders_snapshot (id,run_id,payload_json,published_at)
                    VALUES (1,%s,%s,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE run_id=VALUES(run_id),
                    payload_json=VALUES(payload_json),published_at=VALUES(published_at)""",
                            (run_id, json.dumps(payload, ensure_ascii=False)))
            cur.execute("""UPDATE ofs_repeated_orders_run SET status=%s,error_text=%s,
                finished_at=UTC_TIMESTAMP(6),heartbeat_at=UTC_TIMESTAMP(6) WHERE id=%s""",
                        ("succeeded" if payload is not None else "failed", error, run_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def run_worker_once(repository=None, api_factory=None):
    repository = repository or RepeatedOrdersRepository()
    start = operational_today()
    with repository.worker_lock() as (conn, cur):
        run_id = repository.claim(conn, cur, start)
        if run_id is None:
            return False
        try:
            if api_factory is None:
                from services.ofs_technician_operational_service import OFSOperationalAPI
                api_factory = OFSOperationalAPI
            root = ROOT_RESOURCE_ID
            api = api_factory()
            scope_metrics = {}
            resources = collect_scope(api, lambda: repository.progress(conn, cur, run_id, 0, 0), metrics=scope_metrics)
            counts = [0, 0]
            def progress(pages, count):
                counts[:] = [pages, count]
                repository.progress(conn, cur, run_id, pages, count)
            rows = collect(api, start, root,
                           progress)
            canonical, reconciliation = reconcile_duplicates(
                api, rows, lambda: repository.progress(conn, cur, run_id, *counts))
            payload = analyze(canonical, start, allowed_resources=resources)
            payload["regional_source"] = "activity.stateProvince"
            payload["received"] = len(rows)
            payload["diagnostics"]["duplicate_activity"] += reconciliation["duplicate_activity"]
            payload["diagnostics"]["reconciled_activity"] = reconciliation["reconciled_activity"]
            payload["reconciliation_samples"] = reconciliation["reconciliation_samples"]
            payload["root_resource_id"] = root
            payload["scope_resources_count"] = len(resources)
            payload["scope_metrics"] = scope_metrics
            repository.finish(conn, cur, run_id, payload=payload)
        except Exception as exc:
            repository.finish(conn, cur, run_id, error=sanitize_operational_error(exc)[:500])
        return True
