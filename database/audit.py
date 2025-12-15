import json
from datetime import datetime
from database.connection import get_connection

def audit_log(
    actor_user_id=None,
    actor_username=None,
    module="sistema",
    action="info",
    summary="",
    entity_type=None,
    entity_id=None,
    entity_ref=None,
    before=None,
    after=None,
    meta=None,
):
    """
    before/after/meta podem ser dict/list/str. Serão serializados como JSON.
    """
    def _to_json(x):
        if x is None:
            return None
        if isinstance(x, (dict, list)):
            return json.dumps(x, ensure_ascii=False)
        return str(x)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit_log
        (actor_user_id, actor_username, module, action, entity_type, entity_id, entity_ref,
         summary, before_json, after_json, meta_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            actor_user_id,
            actor_username,
            module,
            action,
            entity_type,
            str(entity_id) if entity_id is not None else None,
            str(entity_ref) if entity_ref is not None else None,
            summary,
            _to_json(before),
            _to_json(after),
            _to_json(meta),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
