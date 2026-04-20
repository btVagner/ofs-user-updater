import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from collections import defaultdict
from urllib.parse import urlencode
from database.connection import get_connection
from ofs.client import OFSClient
from datetime import datetime
BATCH_SIZE_DB = 200
BATCH_SIZE_API = 100

TIPOS_DESCONSIDERADOS = {
    "RET",
    "APR",
    "LUNCH",
    "MAN",
    "PRE",
    "ALM",
    "MAN_VEIC",
    "REUNIAO",
    "EQP_DUP",
}


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def get_pending_ids(limit=BATCH_SIZE_DB):
    today = datetime.now().strftime("%Y-%m-%d")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT activity_id
            FROM ofs_atividades_notdone
            WHERE (activity_type IS NULL OR TRIM(activity_type) = '')
              AND `date` = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (today, limit))
        rows = cur.fetchall()
        return [str(r["activity_id"]).strip() for r in rows if r.get("activity_id")]
    finally:
        cur.close()
        conn.close()

def fetch_activity_types_from_api(client, activity_ids):
    """
    Retorna dict: {activityId: activityType}
    Consulta 1 activity por vez usando /activities/{id}
    """
    result = {}

    for activity_id in activity_ids:
        try:
            url = f"{client.base_url}/activities/{activity_id}"
            data = client.authenticated_get(url)

            found_id = str(data.get("activityId") or activity_id).strip()
            activity_type = str(data.get("activityType") or "").strip().upper()

            if found_id:
                result[found_id] = activity_type or None

        except Exception as e:
            print(f"[WARN] Falha ao buscar activityId {activity_id}: {e}")

    return result

def update_activity_types(type_map):
    if not type_map:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    updated = 0

    try:
        for activity_id, activity_type in type_map.items():
            cur.execute("""
                UPDATE ofs_atividades_notdone
                SET activity_type = %s
                WHERE activity_id = %s
            """, (activity_type, activity_id))
            updated += cur.rowcount

        conn.commit()
        return updated
    finally:
        cur.close()
        conn.close()


def delete_ignored_types():
    conn = get_connection()
    cur = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(TIPOS_DESCONSIDERADOS))
        cur.execute(f"""
            DELETE FROM ofs_atividades_notdone
            WHERE activity_type IN ({placeholders})
        """, tuple(TIPOS_DESCONSIDERADOS))
        deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        cur.close()
        conn.close()


def main():
    client = OFSClient()

    total_found = 0
    total_updated = 0
    total_without_type = 0

    while True:
        pending_ids = get_pending_ids()
        if not pending_ids:
            break

        total_found += len(pending_ids)
        print(f"Lote encontrado no banco: {len(pending_ids)}")

        type_map = fetch_activity_types_from_api(client, pending_ids)
        print(f"Tipos retornados pela API: {len(type_map)}")

        missing_from_api = [aid for aid in pending_ids if aid not in type_map]
        if missing_from_api:
            total_without_type += len(missing_from_api)
            print(f"Sem retorno da API neste lote: {len(missing_from_api)}")

        updated = update_activity_types(type_map)
        total_updated += updated
        print(f"Atualizados no banco neste lote: {updated}")
        print("-" * 60)

        if not type_map:
            # evita loop infinito se a API não retornar nada para o lote
            break

    deleted = delete_ignored_types()

    print("\nBackfill finalizado.")
    print(f"Total lido do banco: {total_found}")
    print(f"Total atualizado com activity_type: {total_updated}")
    print(f"Total sem retorno da API: {total_without_type}")
    print(f"Total removido por tipo desconsiderado: {deleted}")


if __name__ == "__main__":
    main()