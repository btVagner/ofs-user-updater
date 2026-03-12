import traceback
from datetime import datetime, timedelta
from urllib.parse import urlencode

from database.connection import get_connection
from ofs.client import OFSClient
from services.sap_error_parser import parse_sap_error, _extract_message


def validate_max_range_7_days(date_from: str, date_to: str):
    try:
        df = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt = datetime.strptime(date_to, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Informe um período válido (De / Até).")

    if dt < df:
        raise ValueError("O campo 'Até' não pode ser menor que 'De'.")

    if (dt - df).days > 6:
        raise ValueError("Limite máximo: 7 dias por atualização.")

    return df, dt


def job_should_cancel(job_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT cancel_requested, status FROM ofs_import_jobs WHERE id=%s",
        (job_id,),
    )

    row = cur.fetchone() or {}

    cur.close()
    conn.close()

    return int(row.get("cancel_requested") or 0) == 1 or row.get("status") == "canceled"


def job_update(job_id: int, **fields):

    if not fields:
        return

    sets = []
    params = []

    for k, v in fields.items():
        sets.append(f"{k}=%s")
        params.append(v)

    params.append(job_id)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"UPDATE ofs_import_jobs SET {', '.join(sets)} WHERE id=%s",
        tuple(params),
    )

    conn.commit()

    cur.close()
    conn.close()


def iter_days(date_from: str, date_to: str):

    start = datetime.strptime(date_from, "%Y-%m-%d").date()
    end = datetime.strptime(date_to, "%Y-%m-%d").date()

    current = start

    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def run_import_job(job_id: int, date_from: str, date_to: str, resources: str, actor_username: str):

    try:

        client = OFSClient()

        fields = [
            "activityId",
            "city",
            "activityType",
            "apptNumber",
            "date",
            "status",
            "XA_RES_API_NG_RESPONSE",
            "XA_API_NG_DISPATCH",
            "XA_SAP_CRT_LDG",
            "XA_SAP_CRT",
        ]

        days = list(iter_days(date_from, date_to))
        total_days = len(days)

        total_inserted = 0
        total_updated = 0
        total_api_items = 0

        job_update(job_id, message="Iniciando importação...", progress=1)

        sql = """
            INSERT INTO ofs_activities_errors
            (
                activity_id,
                `date`,
                city,
                activity_type,
                appt_number,
                status,
                xa_res_api_ng_response,
                xa_api_ng_dispatch,
                xa_sap_crt_ldg,
                xa_sap_crt,
                sap_error_raw_extracted,
                ng_dispatch_message,
                ng_response_message,
                sap_response_message,
                sap_error_category,
                last_seen_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
                `date`=VALUES(`date`),
                city=VALUES(city),
                activity_type=VALUES(activity_type),
                appt_number=VALUES(appt_number),
                status=VALUES(status),
                xa_res_api_ng_response=VALUES(xa_res_api_ng_response),
                xa_api_ng_dispatch=VALUES(xa_api_ng_dispatch),
                xa_sap_crt_ldg=VALUES(xa_sap_crt_ldg),
                xa_sap_crt=VALUES(xa_sap_crt),
                sap_error_raw_extracted=VALUES(sap_error_raw_extracted),
                ng_dispatch_message=VALUES(ng_dispatch_message),
                ng_response_message=VALUES(ng_response_message),
                sap_response_message=VALUES(sap_response_message),
                sap_error_category=VALUES(sap_error_category),
                last_seen_at=NOW()
        """

        for day_index, day in enumerate(days, start=1):

            if job_should_cancel(job_id):
                job_update(
                    job_id,
                    status="canceled",
                    message="Operação cancelada pelo usuário.",
                    progress=0,
                )
                return

            items = []
            has_more = True
            page = 0
            max_pages = 30
            offset = 0

            while has_more and page < max_pages:

                if job_should_cancel(job_id):
                    job_update(
                        job_id,
                        status="canceled",
                        message="Operação cancelada pelo usuário.",
                        progress=0,
                    )
                    return

                base_params = {
                    "dateFrom": day,
                    "dateTo": day,
                    "resources": resources,
                    "q": "status == 'notdone' OR status == 'completed'",
                    "fields": ",".join(fields),
                    "limit": 2000,
                    "offset": offset,
                }

                qs = urlencode(base_params, safe="=,'")
                url = f"{client.base_url}/activities/?{qs}"

                data = client.authenticated_get(url)

                batch = data.get("items") or []

                items.extend(batch)

                has_more = bool(data.get("hasMore"))
                offset = len(items)
                page += 1

                pct_base = int(((day_index - 1) / max(1, total_days)) * 80)
                pct_page = int((page / max(1, max_pages)) * (80 / max(1, total_days)))

                progress = min(75, pct_base + pct_page + 5)

                job_update(
                    job_id,
                    progress=progress,
                    message=f"API: dia {day} | página {page}/{max_pages}",
                )

            total_api_items += len(items)

            job_update(
                job_id,
                message=f"Gravando dia {day} no banco ({len(items)} itens)...",
                progress=min(85, 5 + int((day_index / max(1, total_days)) * 80)),
            )

            conn = get_connection()
            cur = conn.cursor()

            try:

                cur.execute(
                    """
                    DELETE FROM ofs_activities_errors
                    WHERE `date` = %s
                    """,
                    (day,),
                )

                inserted_day = 0
                updated_day = 0

                for idx, a in enumerate(items, start=1):

                    activity_id = str(a.get("activityId") or "").strip()

                    if not activity_id:
                        continue

                    ng_dispatch_raw = a.get("XA_API_NG_DISPATCH")
                    ng_response_raw = a.get("XA_RES_API_NG_RESPONSE")

                    ng_dispatch_msg = _extract_message(ng_dispatch_raw)
                    ng_response_msg = _extract_message(ng_response_raw)

                    sap_raw = a.get("XA_SAP_CRT_LDG")
                    xa_sap_crt = str(a.get("XA_SAP_CRT") or "").strip() or None

                    sap_info = parse_sap_error(
                        raw_value=sap_raw,
                        xa_sap_crt=xa_sap_crt,
                    )

                    cur.execute(
                        sql,
                        (
                            activity_id,
                            str(a.get("date") or "") or None,
                            str(a.get("city") or "") or None,
                            str(a.get("activityType") or "") or None,
                            str(a.get("apptNumber") or "") or None,
                            str(a.get("status") or "") or None,
                            ng_response_raw,
                            ng_dispatch_raw,
                            sap_raw,
                            xa_sap_crt,
                            sap_info["sap_error_raw_extracted"],
                            ng_dispatch_msg,
                            ng_response_msg,
                            sap_info["sap_response_message"],
                            sap_info["sap_error_category"],
                        ),
                    )

                    if cur.rowcount == 1:
                        inserted_day += 1
                    elif cur.rowcount == 2:
                        updated_day += 1

                conn.commit()

                total_inserted += inserted_day
                total_updated += updated_day

            except Exception:
                conn.rollback()
                raise

            finally:
                cur.close()
                conn.close()

        job_update(
            job_id,
            status="done",
            progress=100,
            message=(
                f"Concluído. Novos: {total_inserted} | "
                f"Atualizados: {total_updated} | "
                f"Total API: {total_api_items}"
            ),
        )

    except Exception as e:

        print(traceback.format_exc(), flush=True)

        job_update(
            job_id,
            status="error",
            message=f"{type(e).__name__}: {str(e)[:220]}",
            progress=0,
        )