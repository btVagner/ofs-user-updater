import json
import os
import time
import threading
import uuid
from datetime import datetime
from typing import Dict, List, Tuple

import requests
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from core.utils import xlsx_auto_width


REQUEST_TIMEOUT = 60
API_LIMIT = 2000

RESOURCE_TYPES = {
    "BK": "Bucket",
    "ESTADO": "Estado",
    "GR": "Grupo",
    "LC": "Loja Comercial",
    "TCP": "Técnico Parceiro",
    "TCW": "Técnico Retirada",
    "TCV": "Técnico Vero",
}

RESOURCE_TYPES_OFS = {
    "BK": "Bucket",
    "GR": "Grupo",
    "LC": "Loja Comercial",
    "TCP": "Técnico Parceiro",
    "TCW": "Técnico Retirada",
    "TCV": "Técnico Vero",
}

ESTADO_RESOURCE_IDS = {
    "GO",
    "MG",
    "MS",
    "SP",
    "SUL",
}

STATUS_OPTIONS = [
    "completed",
    "notdone",
    "cancelled",
    "enroute",
    "pending",
    "started",
    "suspended",
]

CLOSURE_FIELDS = [
    "XA_SER_CLO_PRO_ADA",
    "XA_SER_CLO_IMP_ADA",
    "XA_SER_CLO_PRO_NG",
    "XA_SER_CLO_INP_NG",
]

FIELD_CHOICES = [
    {
        "key": "resourceId",
        "label": "resourceId",
        "api_fields": ["resourceId"],
        "xlsx_header": "ID do técnico",
    },
    {
        "key": "resourceName",
        "label": "Nome do técnico",
        "api_fields": ["resourceId"],
        "xlsx_header": "Nome do técnico",
    },
    {
        "key": "XA_REQ_CRE_DAT",
        "label": "XA_REQ_CRE_DAT",
        "api_fields": ["XA_REQ_CRE_DAT"],
        "xlsx_header": "Data da criação da OS",
    },
    {
        "key": "timeSlot",
        "label": "timeSlot",
        "api_fields": ["timeSlot"],
        "xlsx_header": "Turno",
    },
    {
        "key": "workZone",
        "label": "workZone",
        "api_fields": ["workZone"],
        "xlsx_header": "workZone",
    },
    {
        "key": "stateProvince",
        "label": "stateProvince",
        "api_fields": ["stateProvince"],
        "xlsx_header": "Estado",
    },
    {
        "key": "XA_SAP_CRT_LDG",
        "label": "XA_SAP_CRT_LDG",
        "api_fields": ["XA_SAP_CRT_LDG"],
        "xlsx_header": "Retorno API SAP",
    },
    {
        "key": "date",
        "label": "date",
        "api_fields": ["date"],
        "xlsx_header": "Data",
    },
    {
        "key": "XA_ORG_SYS",
        "label": "XA_ORG_SYS",
        "api_fields": ["XA_ORG_SYS"],
        "xlsx_header": "Sistema de Origem",
    },
    {
        "key": "status",
        "label": "status",
        "api_fields": ["status"],
        "xlsx_header": "Status",
    },
    {
        "key": "customerName",
        "label": "customerName (somente primeiro nome)",
        "api_fields": ["customerName"],
        "xlsx_header": "Primeiro Nome do Cliente",
    },
    {
        "key": "city",
        "label": "city",
        "api_fields": ["city"],
        "xlsx_header": "Cidade",
    },
    {
        "key": "fechamento_atividade",
        "label": "Fechamento da atividade",
        "api_fields": CLOSURE_FIELDS,
        "xlsx_header": "Fechamento da atividade",
    },
    {
        "key": "activityType",
        "label": "activityType",
        "api_fields": ["activityType"],
        "xlsx_header": "Tipo de Atividade",
    },
    {
        "key": "activityId",
        "label": "activityId",
        "api_fields": ["activityId"],
        "xlsx_header": "ID da atividade",
    },
    {
        "key": "apptNumber",
        "label": "apptNumber",
        "api_fields": ["apptNumber"],
        "xlsx_header": "Código da OS",
    },
]

FIELD_MAP = {f["key"]: f for f in FIELD_CHOICES}


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _job_status_path(base_dir: str, job_id: str) -> str:
    return os.path.join(base_dir, f"{job_id}.json")


def _job_xlsx_path(base_dir: str, job_id: str) -> str:
    return os.path.join(base_dir, f"{job_id}.xlsx")


def _write_job_status(base_dir: str, job_id: str, payload: dict):
    _ensure_dir(base_dir)

    path = _job_status_path(base_dir, job_id)

    payload["job_id"] = job_id
    payload["updated_at"] = _now_iso()

    tmp_path = os.path.join(
        base_dir,
        f"{job_id}.{uuid.uuid4().hex}.json.tmp"
    )

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    last_error = None

    for attempt in range(12):
        try:
            os.replace(tmp_path, path)
            return

        except PermissionError as e:
            last_error = e
            time.sleep(0.15 * (attempt + 1))

        except OSError as e:
            last_error = e
            time.sleep(0.15 * (attempt + 1))

    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception:
        pass

    raise last_error


def _load_resource_name_map() -> Dict[str, str]:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT
                resource_id,
                name
            FROM relatorios_ofs_resources
        """)
        rows = cur.fetchall()

        mapping = {}

        for row in rows:
            resource_id = str(row.get("resource_id") or "").strip()
            name = str(row.get("name") or "").strip()

            if not resource_id:
                continue

            mapping[resource_id] = name or "Técnico não encontrado na base"

        return mapping

    finally:
        cur.close()
        conn.close()
def read_job_status(base_dir: str, job_id: str) -> dict:
    path = _job_status_path(base_dir, job_id)

    if not os.path.exists(path):
        return {}

    last_error = None

    for _ in range(5):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            last_error = e
            time.sleep(0.1)

        except PermissionError as e:
            last_error = e
            time.sleep(0.1)

        except OSError as e:
            last_error = e
            time.sleep(0.1)

    print(f"[WARN] Falha ao ler status do job {job_id}: {last_error}")
    return {}
def discard_job(base_dir: str, job_id: str):
    for path in [_job_status_path(base_dir, job_id), _job_xlsx_path(base_dir, job_id)]:
        if os.path.exists(path):
            os.remove(path)


def get_xlsx_path(base_dir: str, job_id: str) -> str:
    return _job_xlsx_path(base_dir, job_id)


def list_resources_grouped() -> Dict[str, List[dict]]:
    grouped = {code: [] for code in RESOURCE_TYPES.keys()}

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT
                resource_id,
                resource_type,
                resource_type_label,
                name,
                status
            FROM relatorios_ofs_resources
            WHERE status = 'active'
            ORDER BY resource_type_label ASC, name ASC, resource_id ASC
        """)
        rows = cur.fetchall()

        for row in rows:
            resource_id = str(row.get("resource_id") or "").strip().upper()
            rtype = str(row.get("resource_type") or "").strip()

            if rtype == "GR" and resource_id in ESTADO_RESOURCE_IDS:
                grouped["ESTADO"].append(row)
                continue

            if rtype in grouped:
                grouped[rtype].append(row)

        return grouped

    finally:
        cur.close()
        conn.close()

def list_activity_types() -> List[dict]:
    """
    Lista tipos de atividade para montar os checkboxes da tela.

    Exibição para o usuário: label_pt, se existir.
    Valor enviado para a API: code.

    A leitura é defensiva para não quebrar caso a tabela não tenha coluna active.
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT *
            FROM ofs_activity_type_map
            ORDER BY code ASC
        """)
        rows = cur.fetchall()

        activity_types = []

        for row in rows:
            code = str(row.get("code") or "").strip()
            if not code:
                continue

            active_value = None
            for active_key in ("active", "ativo", "is_active", "enabled"):
                if active_key in row:
                    active_value = row.get(active_key)
                    break

            if active_value is not None:
                active_str = str(active_value).strip().lower()
                if active_str in {"0", "false", "não", "nao", "no", "inactive", "inativo"}:
                    continue

            label_pt = (
                row.get("label_pt")
                or row.get("descricao")
                or row.get("description")
                or row.get("label")
                or row.get("name")
                or row.get("nome")
                or code
            )

            activity_types.append({
                "code": code,
                "label": str(label_pt or code).strip(),
            })

        return activity_types

    finally:
        cur.close()
        conn.close()
def _normalize_items_payload(data):
    if isinstance(data, dict):
        return data.get("items") or []
    if isinstance(data, list):
        return data
    return []


def sync_resources_from_ofs(actor: dict) -> Tuple[int, int]:
    client = OFSClient()

    url = f"{client.base_url}/resources"
    headers = {"Accept": "application/json"}

    limit = 100
    offset = 0

    all_rows = []

    raw_total = 0
    active_total = 0
    allowed_type_total = 0
    ignored_inactive = 0
    ignored_type = {}

    while True:
        params = {
            "fields": "resourceId,status,resourceType,name",
            "limit": limit,
            "offset": offset,
        }

        resp = requests.get(
            url,
            headers=headers,
            params=params,
            auth=client.auth,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        data = resp.json()
        items = _normalize_items_payload(data)

        if not items:
            break

        raw_total += len(items)

        for item in items:
            resource_id = str(item.get("resourceId") or "").strip()
            status = str(item.get("status") or "").strip()
            resource_type = str(item.get("resourceType") or "").strip()
            name = str(item.get("name") or "").strip()

            if not resource_id:
                continue

            if status.lower() != "active":
                ignored_inactive += 1
                continue

            active_total += 1

            if resource_type not in RESOURCE_TYPES_OFS:
                ignored_type[resource_type or "-"] = ignored_type.get(resource_type or "-", 0) + 1
                continue

            allowed_type_total += 1

            all_rows.append({
                "resource_id": resource_id,
                "resource_type": resource_type,
                "resource_type_label": RESOURCE_TYPES_OFS[resource_type],
                "name": name or None,
                "status": "active",
            })

        if len(items) < limit:
            break

        offset += limit

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM relatorios_ofs_resources")

        sql = """
            INSERT INTO relatorios_ofs_resources
            (
                resource_id,
                resource_type,
                resource_type_label,
                name,
                status
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        for row in all_rows:
            cur.execute(sql, (
                row["resource_id"],
                row["resource_type"],
                row["resource_type_label"],
                row["name"],
                row["status"],
            ))

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    audit_log(
        actor_user_id=actor.get("id"),
        actor_username=actor.get("username"),
        module="relatorios",
        action="sync_ofs_resources",
        entity_type="ofs_resources",
        summary="Atualizou lista de recursos OFS para relatórios",
        meta={
            "raw_total_from_api": raw_total,
            "active_total": active_total,
            "allowed_type_total": allowed_type_total,
            "inserted_total": len(all_rows),
            "ignored_inactive": ignored_inactive,
            "ignored_type": ignored_type,
            "resource_types_allowed": list(RESOURCE_TYPES.keys()),
            "limit": limit,
            "last_offset": offset,
        },
    )

    return len(all_rows), raw_total
def _validate_dates(date_from: str, date_to: str):
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Informe um período válido.")

    if dt_to < dt_from:
        raise ValueError("A data final não pode ser menor que a data inicial.")
def _valid_activity_type_codes() -> set:
    return {item["code"] for item in list_activity_types()}


def _validate_activity_types(activity_types: list) -> List[str]:
    cleaned = []
    valid_codes = _valid_activity_type_codes()

    for activity_type in activity_types:
        value = str(activity_type or "").strip()

        if value in valid_codes and value not in cleaned:
            cleaned.append(value)

    if not cleaned:
        raise ValueError("Selecione pelo menos um tipo de atividade.")

    return cleaned

def _validate_statuses(statuses: list) -> List[str]:
    cleaned = []
    allowed = set(STATUS_OPTIONS)

    for status in statuses:
        value = str(status or "").strip()
        if value in allowed and value not in cleaned:
            cleaned.append(value)

    if not cleaned:
        raise ValueError("Selecione pelo menos um status.")

    return cleaned


def _validate_fields(fields: list) -> List[str]:
    cleaned = []
    allowed = set(FIELD_MAP.keys())

    for field in fields:
        value = str(field or "").strip()
        if value in allowed and value not in cleaned:
            cleaned.append(value)

    if not cleaned:
        raise ValueError("Selecione pelo menos um campo para extração.")

    return cleaned


def _validate_resources(resource_ids: list) -> List[str]:
    selected = []
    for rid in resource_ids:
        value = str(rid or "").strip()
        if value and value not in selected:
            selected.append(value)

    if not selected:
        raise ValueError("Selecione pelo menos um recurso.")

    conn = get_connection()
    cur = conn.cursor()

    try:
        placeholders = ",".join(["%s"] * len(selected))
        cur.execute(f"""
            SELECT resource_id
            FROM relatorios_ofs_resources
            WHERE status = 'active'
              AND resource_id IN ({placeholders})
        """, selected)

        valid = {row[0] for row in cur.fetchall()}

    finally:
        cur.close()
        conn.close()

    invalid = [rid for rid in selected if rid not in valid]
    if invalid:
        raise ValueError("Um ou mais recursos selecionados não existem na lista ativa. Atualize a lista de recursos.")

    return selected


def validate_report_payload(payload: dict) -> dict:
    date_from = str(payload.get("dateFrom") or "").strip()
    date_to = str(payload.get("dateTo") or "").strip()

    _validate_dates(date_from, date_to)

    statuses = _validate_statuses(payload.get("statuses") or [])
    activity_types = _validate_activity_types(payload.get("activityTypes") or [])
    selected_fields = _validate_fields(payload.get("fields") or [])
    resource_ids = _validate_resources(payload.get("resources") or [])

    return {
        "date_from": date_from,
        "date_to": date_to,
        "statuses": statuses,
        "activity_types": activity_types,
        "fields": selected_fields,
        "resources": resource_ids,
    }


def _api_fields_for_selected(selected_fields: List[str]) -> List[str]:
    api_fields = []

    for key in selected_fields:
        cfg = FIELD_MAP[key]
        for api_field in cfg["api_fields"]:
            if api_field not in api_fields:
                api_fields.append(api_field)

    return api_fields


def _first_name(value):
    value = str(value or "").strip()
    if not value:
        return ""
    return value.split()[0]


def _first_filled(item: dict, keys: list):
    for key in keys:
        value = item.get(key)
        if value is None:
            continue

        value_str = str(value).strip()
        if value_str:
            return value_str

    return ""


def _row_value(item: dict, field_key: str, resource_name_map: Dict[str, str] = None):
    if field_key == "customerName":
        return _first_name(item.get("customerName"))

    if field_key == "fechamento_atividade":
        return _first_filled(item, CLOSURE_FIELDS)

    if field_key == "resourceName":
        resource_id = str(item.get("resourceId") or "").strip()
        if not resource_id:
            return "Técnico não encontrado na base"

        if resource_name_map and resource_id in resource_name_map:
            return resource_name_map[resource_id] or "Técnico não encontrado na base"

        return "Técnico não encontrado na base"

    value = item.get(field_key)

    if value is None:
        return ""

    return value
def _build_or_equals_query(field_name: str, values: List[str]) -> str:
    cleaned = []

    for value in values:
        value = str(value or "").strip()
        if not value:
            continue

        value = value.replace("'", "\\'")
        cleaned.append(value)

    if not cleaned:
        raise ValueError(f"Nenhum valor informado para o filtro {field_name}.")

    if len(cleaned) == 1:
        return f"{field_name}=='{cleaned[0]}'"

    parts = [f"{field_name}=='{value}'" for value in cleaned]
    return "(" + " OR ".join(parts) + ")"

def _fetch_activities(client: OFSClient, config: dict, base_dir: str, job_id: str, status_payload: dict) -> List[dict]:
    url = f"{client.base_url}/activities/"
    headers = {"Accept": "application/json"}

    api_fields = _api_fields_for_selected(config["fields"])

    for required_field in ("activityId", "status", "activityType"):
        if required_field not in api_fields:
            api_fields.append(required_field)

    resources_param = ",".join(config["resources"])

    status_query = _build_or_equals_query("status", config["statuses"])
    activity_type_query = _build_or_equals_query("activityType", config["activity_types"])

    # IMPORTANTE:
    # O OFS deve receber apenas UM parâmetro q contendo a expressão completa.
    # Enviar q separado para status e q separado para activityType faz o filtro se comportar errado.
    combined_query = f"{status_query} and {activity_type_query}"

    all_items = []
    seen_activity_ids = set()

    offset = 0
    page = 1

    while True:
        params = [
            ("dateFrom", config["date_from"]),
            ("dateTo", config["date_to"]),
            ("resources", resources_param),
            ("q", combined_query),
            ("fields", ",".join(api_fields)),
            ("limit", str(API_LIMIT)),
            ("offset", str(offset)),
        ]

        status_payload.update({
            "status": "running",
            "phase": f"Consultando OFS - página {page}, offset {offset}",
            "rows_so_far": len(all_items),
            "q": combined_query,
            "offset": offset,
            "page": page,
        })
        _write_job_status(base_dir, job_id, status_payload)

        resp = requests.get(
            url,
            headers=headers,
            params=params,
            auth=client.auth,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        data = resp.json()
        items = _normalize_items_payload(data)

        for item in items:
            activity_id = str(item.get("activityId") or "").strip()

            if activity_id:
                if activity_id in seen_activity_ids:
                    continue
                seen_activity_ids.add(activity_id)

            all_items.append(item)

        has_more = bool(data.get("hasMore")) if isinstance(data, dict) else False

        if not has_more:
            break

        returned_count = len(items)

        if returned_count <= 0:
            raise RuntimeError(
                "A API informou hasMore=true, mas não retornou itens. "
                "A consulta pode estar pesada demais ou excedendo o tempo limite do OFS."
            )

        offset += returned_count
        page += 1

    return all_items


def _build_xlsx(rows: List[dict], selected_fields: List[str], output_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Relatório OS OFS"

    headers = [FIELD_MAP[key]["xlsx_header"] for key in selected_fields]
    ws.append(headers)

    resource_name_map = _load_resource_name_map()

    for item in rows:
        ws.append([
            _row_value(item, key, resource_name_map=resource_name_map)
            for key in selected_fields
        ])

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="D9E2F3")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(vertical="top")

    ws.freeze_panes = "A2"
    xlsx_auto_width(ws)

    wb.save(output_path)
def _run_report_job(base_dir: str, job_id: str, actor: dict, config: dict):
    filename = f"relatorio_os_ofs_{config['date_from']}_{config['date_to']}_{job_id[:8]}.xlsx"

    status_payload = {
        "status": "running",
        "phase": "Iniciando extração",
        "created_at": _now_iso(),
        "finished_at": None,
        "rows_so_far": 0,
        "total_rows": 0,
        "filename": filename,
        "dateFrom": config["date_from"],
        "dateTo": config["date_to"],
        "statuses": config["statuses"],
        "resources": config["resources"],
        "activity_types": config["activity_types"],
        "fields": config["fields"],
        "error": None,
    }

    _write_job_status(base_dir, job_id, status_payload)

    try:
        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="start_ofs_os_report",
            entity_type="report",
            entity_ref=job_id,
            summary="Iniciou extração do Relatório de OS do OFS",
            meta={
                "dateFrom": config["date_from"],
                "dateTo": config["date_to"],
                "statuses": config["statuses"],
                "resources": config["resources"],
                "activity_types": config["activity_types"],
                "fields": config["fields"],
            },
        )

        client = OFSClient()

        rows = _fetch_activities(client, config, base_dir, job_id, status_payload)

        status_payload.update({
            "status": "running",
            "phase": "Gerando XLSX",
            "rows_so_far": len(rows),
            "total_rows": len(rows),
        })
        _write_job_status(base_dir, job_id, status_payload)

        output_path = _job_xlsx_path(base_dir, job_id)
        _build_xlsx(rows, config["fields"], output_path)

        status_payload.update({
            "status": "completed",
            "phase": "Extração concluída",
            "finished_at": _now_iso(),
            "rows_so_far": len(rows),
            "total_rows": len(rows),
            "download_ready": True,
        })
        _write_job_status(base_dir, job_id, status_payload)

        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="finish_ofs_os_report",
            entity_type="report",
            entity_ref=job_id,
            summary="Concluiu extração do Relatório de OS do OFS",
            meta={
                "dateFrom": config["date_from"],
                "dateTo": config["date_to"],
                "statuses": config["statuses"],
                "resources": config["resources"],
                "activity_types": config["activity_types"],
                "fields": config["fields"],
                "total_rows": len(rows),
                "filename": filename,
            },
        )

    except Exception as e:
        status_payload.update({
            "status": "failed",
            "phase": "Falha na extração",
            "finished_at": _now_iso(),
            "error": str(e),
        })
        _write_job_status(base_dir, job_id, status_payload)

        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="fail_ofs_os_report",
            entity_type="report",
            entity_ref=job_id,
            summary="Falha na extração do Relatório de OS do OFS",
            meta={
                "dateFrom": config["date_from"],
                "dateTo": config["date_to"],
                "statuses": config["statuses"],
                "resources": config["resources"],
                "activity_types": config["activity_types"],
                "fields": config["fields"],
                "error": str(e),
            },
        )


def start_report_job(base_dir: str, actor: dict, config: dict) -> str:
    _ensure_dir(base_dir)

    job_id = uuid.uuid4().hex

    initial_payload = {
        "status": "queued",
        "phase": "Extração na fila",
        "created_at": _now_iso(),
        "finished_at": None,
        "rows_so_far": 0,
        "total_rows": 0,
        "filename": None,
        "error": None,
    }
    _write_job_status(base_dir, job_id, initial_payload)

    thread = threading.Thread(
        target=_run_report_job,
        args=(base_dir, job_id, actor, config),
        daemon=True,
    )
    thread.start()

    return job_id

RESOURCE_SYNC_LOCK_STALE_SECONDS = 6 * 60 * 60


def _resource_sync_lock_path(base_dir: str) -> str:
    return os.path.join(base_dir, "resource_sync.lock")


def _read_resource_sync_lock(base_dir: str) -> dict:
    path = _resource_sync_lock_path(base_dir)
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _remove_resource_sync_lock(base_dir: str):
    path = _resource_sync_lock_path(base_dir)
    if os.path.exists(path):
        os.remove(path)


def _lock_is_stale(lock_payload: dict) -> bool:
    created_ts = float(lock_payload.get("created_ts") or 0)
    if not created_ts:
        return True

    return (time.time() - created_ts) > RESOURCE_SYNC_LOCK_STALE_SECONDS


def _try_create_resource_sync_lock(base_dir: str, job_id: str, actor: dict):
    _ensure_dir(base_dir)

    lock_path = _resource_sync_lock_path(base_dir)
    current_lock = _read_resource_sync_lock(base_dir)

    if current_lock and _lock_is_stale(current_lock):
        _remove_resource_sync_lock(base_dir)

    payload = {
        "job_id": job_id,
        "actor_user_id": actor.get("id"),
        "actor_username": actor.get("username"),
        "created_at": _now_iso(),
        "created_ts": time.time(),
    }

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return True, None

    except FileExistsError:
        return False, _read_resource_sync_lock(base_dir)


def _extract_total_from_resource_payload(data: dict):
    if not isinstance(data, dict):
        return None

    for key in ("totalResults", "total", "totalCount", "count"):
        value = data.get(key)
        try:
            value_int = int(value)
            if value_int > 0:
                return value_int
        except Exception:
            pass

    return None


def _resource_percent(raw_total: int, total_expected):
    if total_expected:
        return min(95, int((raw_total / total_expected) * 100))

    # Fallback quando a API não informa total.
    # Não é percentual real, mas evita a barra parada até finalizar.
    return min(95, max(5, int(raw_total / 30)))


def _sync_resources_from_ofs_with_progress(base_dir: str, job_id: str, actor: dict, status_payload: dict):
    client = OFSClient()

    url = f"{client.base_url}/resources"
    headers = {"Accept": "application/json"}

    limit = 100
    offset = 0
    page = 1
    max_pages = 500

    all_rows = []

    raw_total = 0
    active_total = 0
    allowed_type_total = 0
    ignored_inactive = 0
    ignored_type = {}
    total_expected = None

    while True:
        params = {
            "fields": "resourceId,status,resourceType,name",
            "limit": limit,
            "offset": offset,
        }

        status_payload.update({
            "status": "running",
            "phase": f"Consultando recursos no OFS - página {page}",
            "percent": _resource_percent(raw_total, total_expected),
            "raw_total_from_api": raw_total,
            "inserted_so_far": len(all_rows),
            "page": page,
            "offset": offset,
            "total_expected": total_expected,
        })
        _write_job_status(base_dir, job_id, status_payload)

        resp = requests.get(
            url,
            headers=headers,
            params=params,
            auth=client.auth,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        data = resp.json()

        if total_expected is None:
            total_expected = _extract_total_from_resource_payload(data)

        items = _normalize_items_payload(data)

        if not items:
            break

        raw_total += len(items)

        for item in items:
            resource_id = str(item.get("resourceId") or "").strip()
            status = str(item.get("status") or "").strip()
            resource_type = str(item.get("resourceType") or "").strip()
            name = str(item.get("name") or "").strip()

            if not resource_id:
                continue

            if status.lower() != "active":
                ignored_inactive += 1
                continue

            active_total += 1

            if resource_type not in RESOURCE_TYPES_OFS:
                ignored_type[resource_type or "-"] = ignored_type.get(resource_type or "-", 0) + 1
                continue

            allowed_type_total += 1

            all_rows.append({
                "resource_id": resource_id,
                "resource_type": resource_type,
                "resource_type_label": RESOURCE_TYPES_OFS[resource_type],
                "name": name or None,
                "status": "active",
            })

        status_payload.update({
            "status": "running",
            "phase": f"Processando recursos - página {page}",
            "percent": _resource_percent(raw_total, total_expected),
            "raw_total_from_api": raw_total,
            "inserted_so_far": len(all_rows),
            "page": page,
            "offset": offset,
            "total_expected": total_expected,
        })
        _write_job_status(base_dir, job_id, status_payload)

        if len(items) < limit:
            break

        offset += limit
        page += 1

        if page > max_pages:
            raise RuntimeError("Limite máximo de páginas atingido ao consultar recursos OFS.")

    status_payload.update({
        "status": "running",
        "phase": "Atualizando tabela de recursos no banco",
        "percent": 96,
        "raw_total_from_api": raw_total,
        "inserted_so_far": len(all_rows),
    })
    _write_job_status(base_dir, job_id, status_payload)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM relatorios_ofs_resources")

        sql = """
            INSERT INTO relatorios_ofs_resources
            (
                resource_id,
                resource_type,
                resource_type_label,
                name,
                status
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        values = [
            (
                row["resource_id"],
                row["resource_type"],
                row["resource_type_label"],
                row["name"],
                row["status"],
            )
            for row in all_rows
        ]

        if values:
            cur.executemany(sql, values)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    return {
        "raw_total_from_api": raw_total,
        "active_total": active_total,
        "allowed_type_total": allowed_type_total,
        "inserted_total": len(all_rows),
        "ignored_inactive": ignored_inactive,
        "ignored_type": ignored_type,
        "resource_types_allowed": list(RESOURCE_TYPES.keys()),
        "limit": limit,
        "last_offset": offset,
        "total_expected": total_expected,
    }


def _run_resource_sync_job(base_dir: str, job_id: str, actor: dict):
    status_payload = {
        "status": "running",
        "phase": "Iniciando atualização da lista de recursos",
        "created_at": _now_iso(),
        "finished_at": None,
        "percent": 0,
        "raw_total_from_api": 0,
        "inserted_so_far": 0,
        "inserted_total": 0,
        "error": None,
    }

    _write_job_status(base_dir, job_id, status_payload)

    try:
        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="start_sync_ofs_resources",
            entity_type="ofs_resources",
            entity_ref=job_id,
            summary="Iniciou atualização da lista de recursos OFS em segundo plano",
            meta={
                "resource_types": list(RESOURCE_TYPES.keys()),
            },
        )

        result = _sync_resources_from_ofs_with_progress(base_dir, job_id, actor, status_payload)

        status_payload.update({
            "status": "completed",
            "phase": "Lista de recursos atualizada com sucesso",
            "finished_at": _now_iso(),
            "percent": 100,
            "raw_total_from_api": result.get("raw_total_from_api", 0),
            "inserted_total": result.get("inserted_total", 0),
            "inserted_so_far": result.get("inserted_total", 0),
            "result": result,
        })
        _write_job_status(base_dir, job_id, status_payload)

        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="finish_sync_ofs_resources",
            entity_type="ofs_resources",
            entity_ref=job_id,
            summary="Concluiu atualização da lista de recursos OFS",
            meta=result,
        )

    except Exception as e:
        status_payload.update({
            "status": "failed",
            "phase": "Falha ao atualizar lista de recursos",
            "finished_at": _now_iso(),
            "error": str(e),
        })
        _write_job_status(base_dir, job_id, status_payload)

        audit_log(
            actor_user_id=actor.get("id"),
            actor_username=actor.get("username"),
            module="relatorios",
            action="fail_sync_ofs_resources",
            entity_type="ofs_resources",
            entity_ref=job_id,
            summary="Falha ao atualizar lista de recursos OFS",
            meta={
                "error": str(e),
            },
        )

    finally:
        _remove_resource_sync_lock(base_dir)


def start_resource_sync_job(base_dir: str, actor: dict):
    _ensure_dir(base_dir)

    job_id = uuid.uuid4().hex

    locked, active_lock = _try_create_resource_sync_lock(base_dir, job_id, actor)

    if not locked:
        return None, active_lock

    initial_payload = {
        "status": "queued",
        "phase": "Atualização de recursos na fila",
        "created_at": _now_iso(),
        "finished_at": None,
        "percent": 0,
        "raw_total_from_api": 0,
        "inserted_so_far": 0,
        "inserted_total": 0,
        "error": None,
    }
    _write_job_status(base_dir, job_id, initial_payload)

    thread = threading.Thread(
        target=_run_resource_sync_job,
        args=(base_dir, job_id, actor),
        daemon=True,
    )
    thread.start()

    return job_id, None


def read_resource_sync_job_status(base_dir: str, job_id: str) -> dict:
    return read_job_status(base_dir, job_id)