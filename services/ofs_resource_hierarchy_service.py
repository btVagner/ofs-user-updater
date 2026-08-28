from contextlib import contextmanager
from datetime import datetime, timezone
import re
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

from database.connection import get_connection
from ofs.client import OFSClient
from ofs.config import get_ofs_root_resource_id


PAGE_LIMIT = 100
MAX_PAGES = 1000
LOCK_NAME = "ofs_resource_hierarchy_sync"
SYNC_SOURCE_NAME = "hierarchy"
RESOURCE_FIELDS = (
    "resourceId",
    "parentResourceId",
    "name",
    "resourceType",
    "status",
    "timeZone",
)


class HierarchySyncError(RuntimeError):
    pass


class HierarchySyncAlreadyRunning(HierarchySyncError):
    pass


def _clean(value) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _http_status(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def _safe_error_code(exc: Exception) -> str:
    status = _http_status(exc)
    if status is not None:
        return f"OFS_HTTP_{status}"[:64]
    name = re.sub(r"[^A-Z0-9_]+", "_", exc.__class__.__name__.upper()).strip("_")
    return (name or "HIERARCHY_SYNC_ERROR")[:64]


def _safe_error_text(exc: Exception) -> str:
    status = _http_status(exc)
    if status is not None:
        return f"Falha HTTP {status} durante a sincronização da hierarquia OFS."

    text = str(exc or "").strip()
    if not text:
        return "Falha durante a sincronização da hierarquia OFS."

    # O erro persistido pode ser exibido pela API local. Evite carregar URLs,
    # query strings e valores com nomes típicos de credenciais para o banco/UI.
    text = re.sub(r"https?://\S+", "<url>", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?i)\b(password|passwd|token|access_token|client_secret|secret|authorization|assertion)\b\s*[:=]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )
    return text[:500]


def _is_fields_compatibility_error(exc: Exception) -> bool:
    return isinstance(exc, requests.HTTPError) and _http_status(exc) in {400, 409}


def _build_url(base_url: str, path: str, params: Optional[dict] = None) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def _get_resource(client: OFSClient, resource_id: str) -> Tuple[dict, int]:
    params = {"fields": ",".join(RESOURCE_FIELDS)}
    url = _build_url(client.base_url, f"resources/{resource_id}", params)
    try:
        return client.authenticated_get(url), 1
    except Exception as exc:
        if not _is_fields_compatibility_error(exc):
            raise

    # Compatibilidade com ambientes que rejeitem fields neste endpoint.
    url = _build_url(client.base_url, f"resources/{resource_id}")
    return client.authenticated_get(url), 2


def _get_descendants_page(
    client: OFSClient,
    root_resource_id: str,
    offset: int,
    *,
    include_fields: bool,
) -> dict:
    params = {
        "limit": PAGE_LIMIT,
        "offset": offset,
    }
    if include_fields:
        params["fields"] = ",".join(RESOURCE_FIELDS)

    url = _build_url(
        client.base_url,
        f"resources/{root_resource_id}/descendants",
        params,
    )
    return client.authenticated_get(url)


def fetch_resource_hierarchy_snapshot(
    client: Optional[OFSClient] = None,
    root_resource_id: Optional[str] = None,
) -> dict:
    """Coleta root + todos os descendentes sem escrever no banco.

    Só retorna com sucesso quando todas as páginas esperadas foram obtidas.
    Qualquer falha interrompe a rotina antes do UPSERT, preservando o snapshot
    local anterior.
    """
    client = client or OFSClient()
    root_resource_id = _clean(root_resource_id) or get_ofs_root_resource_id()
    if not root_resource_id:
        raise HierarchySyncError("Root da hierarquia OFS não configurado.")

    root_payload, api_calls = _get_resource(client, root_resource_id)
    root_id_from_api = _clean(root_payload.get("resourceId"))
    if root_id_from_api and root_id_from_api != root_resource_id:
        raise HierarchySyncError(
            f"O root retornado pela API ({root_id_from_api}) difere do configurado ({root_resource_id})."
        )

    descendants: List[dict] = []
    offset = 0
    page = 1
    total_expected: Optional[int] = None
    include_fields = True
    fields_fallback_used = False

    while True:
        if page > MAX_PAGES:
            raise HierarchySyncError("Limite de segurança de páginas excedido na hierarquia OFS.")

        try:
            payload = _get_descendants_page(
                client,
                root_resource_id,
                offset,
                include_fields=include_fields,
            )
            api_calls += 1
        except Exception as exc:
            if page == 1 and include_fields and _is_fields_compatibility_error(exc):
                include_fields = False
                fields_fallback_used = True
                payload = _get_descendants_page(
                    client,
                    root_resource_id,
                    offset,
                    include_fields=False,
                )
                api_calls += 2  # tentativa com fields + retry compatível
            else:
                raise

        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise HierarchySyncError("Resposta inválida da API de descendentes: items ausente ou inválido.")

        if total_expected is None:
            raw_total = payload.get("totalResults") if isinstance(payload, dict) else None
            try:
                total_expected = int(raw_total) if raw_total is not None else None
            except (TypeError, ValueError):
                total_expected = None
            if total_expected is not None and total_expected < 0:
                raise HierarchySyncError("totalResults inválido na API de descendentes.")

        descendants.extend(items)

        if total_expected is not None:
            if len(descendants) >= total_expected:
                if len(descendants) != total_expected:
                    raise HierarchySyncError(
                        "Paginação inconsistente: quantidade coletada difere de totalResults."
                    )
                break
            if not items:
                raise HierarchySyncError(
                    "Paginação incompleta: API retornou página vazia antes de totalResults."
                )
        else:
            if len(items) < PAGE_LIMIT:
                break
            if not items:
                break

        offset += len(items)
        page += 1

    status_recheck_cache: Dict[str, Optional[str]] = {}
    status_rechecks_total = 0
    status_recheck_api_calls = 0

    def recheck_resource_status(resource_id: str) -> Optional[str]:
        nonlocal status_rechecks_total, status_recheck_api_calls
        resource_id = _clean(resource_id)
        if not resource_id:
            return None
        if resource_id in status_recheck_cache:
            return status_recheck_cache[resource_id]

        try:
            payload, calls = _get_resource(client, resource_id)
        except requests.HTTPError as exc:
            if _http_status(exc) != 404:
                raise
            status_rechecks_total += 1
            status_recheck_api_calls += 1
            status_recheck_cache[resource_id] = None
            return None

        status_rechecks_total += 1
        status_recheck_api_calls += calls
        status = (_clean(payload.get("status")) or "unknown").lower()
        status_recheck_cache[resource_id] = status
        return status

    (
        rows,
        skipped_without_resource_id,
        skipped_inactive_status,
        skipped_under_inactive_ancestor,
        inactive_confirmed_by_recheck,
    ) = build_hierarchy_rows(
        root_payload,
        descendants,
        root_resource_id,
        status_resolver=recheck_resource_status,
    )
    api_calls += status_recheck_api_calls
    return {
        "root_resource_id": root_resource_id,
        "rows": rows,
        "descendants_total": len(descendants),
        "resources_total": len(rows),
        "pages": page,
        "api_calls": api_calls,
        "total_results": total_expected,
        "fields_fallback_used": fields_fallback_used,
        "skipped_without_resource_id": skipped_without_resource_id,
        "skipped_inactive_status": skipped_inactive_status,
        "skipped_under_inactive_ancestor": skipped_under_inactive_ancestor,
        "status_rechecks_total": status_rechecks_total,
        "inactive_confirmed_by_recheck": inactive_confirmed_by_recheck,
    }


def _resource_id(item: dict) -> Optional[str]:
    if not isinstance(item, dict):
        return None
    return _clean(
        item.get("resourceId")
        or item.get("id")
        or item.get("resource_id")
    )


def _normalize_resource(item: dict, root_resource_id: str) -> dict:
    resource_id = _resource_id(item)
    if not resource_id:
        raise HierarchySyncError("Recurso sem resourceId utilizável recebido da API OFS.")

    return {
        "resource_id": resource_id,
        "parent_resource_id": _clean(item.get("parentResourceId")),
        "resource_name": _clean(item.get("name")) or resource_id,
        "resource_type": _clean(item.get("resourceType")),
        "status": (_clean(item.get("status")) or "unknown").lower(),
        "timezone": _clean(item.get("timeZone") or item.get("timezone")),
        "root_resource_id": root_resource_id,
        "depth": None,
    }


def build_hierarchy_rows(
    root_payload: dict,
    descendants: Iterable[dict],
    root_resource_id: str,
    *,
    status_resolver: Optional[Callable[[str], Optional[str]]] = None,
) -> Tuple[List[dict], int, int, int, int]:
    """Monta a árvore cadastral ativa usando parentResourceId.

    ``resource.status == inactive`` representa recurso/conta desativada no
    cadastro OFS e não deve integrar a árvore operacional local. Isso é
    diferente de um técnico ativo sem jornada no dia, conceito que pertence
    ao read model operacional (Demandas 05/06).

    Recursos ativos sob um ancestral cadastralmente inativo também são
    podados, preservando uma árvore persistida íntegra sem reparentear dados
    em desacordo com o parentResourceId real.
    """
    root_resource_id = _clean(root_resource_id)
    if not root_resource_id:
        raise HierarchySyncError("Root inválido para construção da hierarquia.")

    all_by_id: Dict[str, dict] = {}

    root_row = _normalize_resource(
        {**(root_payload or {}), "resourceId": root_resource_id},
        root_resource_id,
    )
    root_row["depth"] = 0
    all_by_id[root_resource_id] = root_row

    skipped_without_resource_id = 0
    skipped_inactive_status = 0
    inactive_confirmed_by_recheck = 0

    for item in descendants or []:
        if not isinstance(item, dict):
            raise HierarchySyncError("Item inválido recebido na lista de descendentes OFS.")
        if not _resource_id(item):
            skipped_without_resource_id += 1
            continue

        row = _normalize_resource(item, root_resource_id)
        if row["resource_id"] == root_resource_id:
            merged = dict(root_row)
            merged.update({key: value for key, value in row.items() if value is not None})
            merged["depth"] = 0
            all_by_id[root_resource_id] = merged
            root_row = merged
            continue

        all_by_id[row["resource_id"]] = row
        if row["status"] == "inactive":
            skipped_inactive_status += 1

    eligibility_cache: Dict[str, bool] = {root_resource_id: True}
    depth_cache: Dict[str, int] = {root_resource_id: 0}
    skipped_under_inactive_ancestor = 0

    def authoritative_status(resource_id: str) -> Optional[str]:
        nonlocal skipped_inactive_status, inactive_confirmed_by_recheck
        if status_resolver is None:
            return None
        status = status_resolver(resource_id)
        status = (_clean(status) or "unknown").lower()
        current = all_by_id.get(resource_id)
        if current is not None and status != "unknown":
            previous = current.get("status")
            current["status"] = status
            if status == "inactive" and previous != "inactive":
                skipped_inactive_status += 1
                inactive_confirmed_by_recheck += 1
        return status

    def resolve_eligibility_and_depth(resource_id: str) -> Tuple[bool, Optional[int]]:
        if resource_id in eligibility_cache:
            return eligibility_cache[resource_id], depth_cache.get(resource_id)

        current_id = resource_id
        visited: List[str] = []
        visited_set = set()

        while True:
            if current_id in visited_set:
                raise HierarchySyncError(f"Ciclo detectado na hierarquia em {resource_id}.")
            visited_set.add(current_id)

            if current_id in eligibility_cache:
                eligible = eligibility_cache[current_id]
                base_depth = depth_cache.get(current_id)
                break

            current = all_by_id.get(current_id)
            if not current:
                raise HierarchySyncError(
                    f"Cadeia hierárquica incompleta: recurso {current_id} não foi coletado."
                )

            # Conta/recurso cadastralmente desativado não participa do monitor.
            # Sua cadeia histórica pode estar quebrada e não deve invalidar o
            # snapshot dos recursos ainda ativos.
            if current["status"] == "inactive":
                eligible = False
                base_depth = None
                eligibility_cache[current_id] = False
                depth_cache.pop(current_id, None)
                break

            visited.append(current_id)
            parent_id = current.get("parent_resource_id")
            if not parent_id:
                # Alguns ambientes podem devolver status incompleto/desatualizado
                # em /descendants para recursos legados desativados. Antes de
                # considerar a árvore inválida, confirme pontualmente o recurso.
                if authoritative_status(current_id) == "inactive":
                    eligibility_cache[current_id] = False
                    depth_cache.pop(current_id, None)
                    eligible = False
                    base_depth = None
                    break
                raise HierarchySyncError(
                    f"Cadeia hierárquica de {resource_id} não alcança o root {root_resource_id}."
                )

            if parent_id != root_resource_id and parent_id not in all_by_id:
                # Primeiro confirme o próprio nó: se ele foi desativado, todo o
                # ramo abaixo deve ser podado e a cadeia histórica pode ser ignorada.
                if authoritative_status(current_id) == "inactive":
                    eligibility_cache[current_id] = False
                    depth_cache.pop(current_id, None)
                    eligible = False
                    base_depth = None
                    break

                # Se o pai ausente ainda existir no OFS e estiver inativo, ele é
                # justamente o corte do ramo. Não o persista nem reparentear filhos.
                if authoritative_status(parent_id) == "inactive":
                    eligible = False
                    base_depth = None
                    break

                raise HierarchySyncError(
                    f"Pai {parent_id} de {resource_id} não está no snapshot coletado."
                )

            current_id = parent_id

        if not eligible:
            for node_id in visited:
                eligibility_cache[node_id] = False
                depth_cache.pop(node_id, None)
            return False, None

        assert base_depth is not None
        depth = base_depth
        for node_id in reversed(visited):
            depth += 1
            eligibility_cache[node_id] = True
            depth_cache[node_id] = depth

        return True, depth_cache[resource_id]

    persisted_by_id: Dict[str, dict] = {root_resource_id: dict(root_row)}
    persisted_by_id[root_resource_id]["depth"] = 0

    for resource_id, row in all_by_id.items():
        if resource_id == root_resource_id or row["status"] == "inactive":
            continue

        eligible, depth = resolve_eligibility_and_depth(resource_id)
        if not eligible:
            # Se o próprio recurso foi confirmado inativo no recheck, ele já
            # está contabilizado em skipped_inactive_status. Apenas filhos
            # ativos podados pelo ancestral entram neste contador.
            if row.get("status") != "inactive":
                skipped_under_inactive_ancestor += 1
            continue

        persisted = dict(row)
        persisted["depth"] = depth
        persisted_by_id[resource_id] = persisted

    return (
        sorted(
            persisted_by_id.values(),
            key=lambda item: (item["depth"], item["resource_id"]),
        ),
        skipped_without_resource_id,
        skipped_inactive_status,
        skipped_under_inactive_ancestor,
        inactive_confirmed_by_recheck,
    )


class MySQLHierarchyRepository:
    UPSERT_SQL = """
        INSERT INTO ofs_resource_hierarchy
        (
            resource_id,
            parent_resource_id,
            resource_name,
            resource_type,
            status,
            timezone,
            depth,
            root_resource_id,
            last_seen_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            parent_resource_id = VALUES(parent_resource_id),
            resource_name = VALUES(resource_name),
            resource_type = VALUES(resource_type),
            status = VALUES(status),
            timezone = VALUES(timezone),
            depth = VALUES(depth),
            root_resource_id = VALUES(root_resource_id),
            last_seen_at = VALUES(last_seen_at),
            updated_at = VALUES(updated_at)
    """

    def __init__(self, connection_factory: Callable = get_connection):
        self.connection_factory = connection_factory

    def update_sync_status(
        self,
        *,
        status: str,
        started_at: Optional[datetime] = None,
        success_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO ofs_operational_sync_state
                    (source_name,last_started_at,last_success_at,last_finished_at,status,error_code,error_message,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    last_started_at=COALESCE(VALUES(last_started_at),last_started_at),
                    last_success_at=COALESCE(VALUES(last_success_at),last_success_at),
                    last_finished_at=COALESCE(VALUES(last_finished_at),last_finished_at),
                    status=VALUES(status),
                    error_code=VALUES(error_code),
                    error_message=VALUES(error_message),
                    updated_at=VALUES(updated_at)
                """,
                (
                    SYNC_SOURCE_NAME,
                    started_at,
                    success_at,
                    finished_at,
                    status,
                    error_code,
                    error_message,
                    _utc_now_naive(),
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def replace_snapshot(self, rows: List[dict], root_resource_id: str, seen_at: datetime) -> int:
        if not rows:
            raise HierarchySyncError("Snapshot vazio não será persistido.")

        conn = self.connection_factory()
        cur = conn.cursor()
        try:
            values = [
                (
                    row["resource_id"],
                    row.get("parent_resource_id"),
                    row["resource_name"],
                    row.get("resource_type"),
                    row["status"],
                    row.get("timezone"),
                    row["depth"],
                    root_resource_id,
                    seen_at,
                    seen_at,
                )
                for row in rows
            ]
            cur.executemany(self.UPSERT_SQL, values)

            # Somente depois da coleta integral e do UPSERT bem-sucedido os
            # recursos ausentes deixam de representar o estado atual.
            cur.execute(
                """
                DELETE FROM ofs_resource_hierarchy
                WHERE root_resource_id = %s
                  AND last_seen_at <> %s
                """,
                (root_resource_id, seen_at),
            )
            removed = max(int(cur.rowcount or 0), 0)
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()


@contextmanager
def mysql_sync_lock(lock_name: str = LOCK_NAME, connection_factory: Callable = get_connection):
    conn = connection_factory()
    cur = conn.cursor()
    acquired = False
    try:
        cur.execute("SELECT GET_LOCK(%s, 0)", (lock_name,))
        row = cur.fetchone()
        acquired = bool(row and row[0] == 1)
        if not acquired:
            raise HierarchySyncAlreadyRunning("Já existe uma sincronização da hierarquia OFS em execução.")
        yield
    finally:
        try:
            if acquired:
                cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                cur.fetchone()
        finally:
            cur.close()
            conn.close()


def _sync_without_lock(
    *,
    client: Optional[OFSClient],
    root_resource_id: Optional[str],
    repository,
    seen_at: datetime,
) -> dict:
    snapshot = fetch_resource_hierarchy_snapshot(
        client=client,
        root_resource_id=root_resource_id,
    )
    removed = repository.replace_snapshot(
        snapshot["rows"],
        snapshot["root_resource_id"],
        seen_at,
    )

    active_total = sum(1 for row in snapshot["rows"] if row["status"] == "active")
    inactive_total = snapshot["resources_total"] - active_total
    max_depth = max((row["depth"] for row in snapshot["rows"]), default=0)
    depth_counts = {}
    resource_type_counts = {}
    for row in snapshot["rows"]:
        depth_key = str(row["depth"])
        type_key = row.get("resource_type") or "-"
        depth_counts[depth_key] = depth_counts.get(depth_key, 0) + 1
        resource_type_counts[type_key] = resource_type_counts.get(type_key, 0) + 1

    return {
        **{key: value for key, value in snapshot.items() if key != "rows"},
        "active_total": active_total,
        "inactive_total": inactive_total,
        "max_depth": max_depth,
        "depth_counts": depth_counts,
        "resource_type_counts": resource_type_counts,
        "removed_total": removed,
        "seen_at_utc": seen_at.isoformat(timespec="seconds"),
    }


def sync_resource_hierarchy(
    *,
    client: Optional[OFSClient] = None,
    root_resource_id: Optional[str] = None,
    repository=None,
    use_lock: bool = True,
    seen_at: Optional[datetime] = None,
) -> dict:
    """Sincroniza o estado atual da hierarquia OFS de forma idempotente.

    Quando o repositório oferece ``update_sync_status`` (caso padrão MySQL),
    a execução também publica health estrutural em ``ofs_operational_sync_state``
    usando a fonte isolada ``hierarchy``. As regras operacionais continuam
    filtrando apenas Events/Activities/Calendars/Routes.
    """
    repository = repository or MySQLHierarchyRepository()
    seen_at = seen_at or _utc_now_naive()

    def run_once() -> dict:
        started_at = _utc_now_naive()
        update_status = getattr(repository, "update_sync_status", None)
        if callable(update_status):
            update_status(status="running", started_at=started_at)

        try:
            result = _sync_without_lock(
                client=client,
                root_resource_id=root_resource_id,
                repository=repository,
                seen_at=seen_at,
            )
        except Exception as exc:
            finished_at = _utc_now_naive()
            if callable(update_status):
                try:
                    update_status(
                        status="error",
                        finished_at=finished_at,
                        error_code=_safe_error_code(exc),
                        error_message=_safe_error_text(exc),
                    )
                except Exception:
                    # Não esconda a causa original caso a própria persistência
                    # de health também esteja indisponível.
                    pass
            raise

        finished_at = _utc_now_naive()
        if callable(update_status):
            update_status(
                status="ok",
                success_at=finished_at,
                finished_at=finished_at,
                error_code=None,
                error_message=None,
            )
        return {
            **result,
            "hierarchy_sync_status": "ok",
            "hierarchy_last_success_at_utc": finished_at.isoformat(timespec="seconds"),
            "sync_started_at_utc": started_at.isoformat(timespec="seconds"),
            "sync_finished_at_utc": finished_at.isoformat(timespec="seconds"),
        }

    if use_lock:
        with mysql_sync_lock():
            return run_once()

    return run_once()
