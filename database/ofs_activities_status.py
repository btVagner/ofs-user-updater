# database/ofs_activities_status.py
from database.connection import get_connection

def upsert_activities(rows: list[dict]) -> int:
    if not rows:
        return 0

    conn = get_connection()
    cur = conn.cursor()

    sql = """
        INSERT INTO ofs_activities_status (
            activity_id, date, city, activity_type, appt_number, status,
            XA_RES_API_NG_RESPONSE, XA_API_NG_DISPATCH, XA_SAP_CRT_LDG, XA_SAP_CRT, imported_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
        ON DUPLICATE KEY UPDATE
            city=VALUES(city),
            activity_type=VALUES(activity_type),
            appt_number=VALUES(appt_number),
            status=VALUES(status),
            XA_RES_API_NG_RESPONSE=VALUES(XA_RES_API_NG_RESPONSE),
            XA_API_NG_DISPATCH=VALUES(XA_API_NG_DISPATCH),
            XA_SAP_CRT_LDG=VALUES(XA_SAP_CRT_LDG),
            XA_SAP_CRT=VALUES(XA_SAP_CRT),
            imported_at=NOW()
    """

    data = []
    for r in rows:
        data.append((
            r.get("activityId"),
            r.get("date"),
            r.get("city"),
            r.get("activityType"),
            r.get("apptNumber"),
            r.get("status"),
            r.get("XA_RES_API_NG_RESPONSE"),
            r.get("XA_API_NG_DISPATCH"),
            r.get("XA_SAP_CRT_LDG"),
            r.get("XA_SAP_CRT"),
        ))

    cur.executemany(sql, data)
    affected = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    return affected


def list_activities(date_from: str, date_to: str, resources: str | None = None) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            activity_id AS activityId,
            `date` AS `date`,
            city,
            activity_type AS activityType,
            appt_number AS apptNumber,
            `status` AS status,
            XA_RES_API_NG_RESPONSE,
            XA_API_NG_DISPATCH,
            XA_SAP_CRT_LDG,
            XA_SAP_CRT
        FROM ofs_activities_status
        WHERE `date` BETWEEN %s AND %s
        ORDER BY `date` DESC, activity_id DESC
    """, (date_from, date_to))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_ldg(activity_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            activity_id AS activityId,
            XA_SAP_CRT_LDG,
            XA_SAP_CRT
        FROM ofs_activities_status
        WHERE activity_id = %s
        ORDER BY imported_at DESC
        LIMIT 1
    """, (activity_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()
    return row