import os
import json
import datetime as dt
import requests
from flask import Blueprint, render_template, request, current_app

bp_activities_notdone = Blueprint("activities_notdone", __name__)

FIELDS = [
    "city",
    "customerNumber",
    "customerName",
    "customerPhone",
    "apptNumber",
    "XA_ORIGIN_BUCKET",
    "XA_TSK_NOT",
    "XA_SER_CLO_IMP_ADA",
    "resourceId",
    "date",
]

def _ofs_auth():
    """
    🔧 AJUSTAR conforme seu projeto.
    Opção A (basic): user = "<CLIENT-ID>@<INSTANCE-NAME>", pass = "<CLIENT-SECRET>"
    Opção B: se você já tem bearer token, troque por headers Authorization.
    """
    ofs_user = os.getenv("OFS_BASIC_USER")  # ex: CLIENTID@INSTANCE
    ofs_pass = os.getenv("OFS_BASIC_PASS")  # ex: CLIENTSECRET
    if not ofs_user or not ofs_pass:
        raise RuntimeError("Credenciais OFS_BASIC_USER/OFS_BASIC_PASS não configuradas no .env")
    return (ofs_user, ofs_pass)

def _ofs_get(path: str, params: dict):
    base = os.getenv("OFS_BASE_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("OFS_BASE_URL não configurada no .env")

    url = f"{base}/{path.lstrip('/')}"
    auth = _ofs_auth()

    # Importante: timeout curto pra não travar request do Flask
    resp = requests.get(url, params=params, auth=auth, timeout=30)
    if resp.status_code >= 400:
        current_app.logger.error("OFS GET ERROR %s - %s", resp.status_code, resp.text)
        resp.raise_for_status()
    return resp.json()

@bp_activities_notdone.get("/activities/notdone")
def activities_notdone():
    # Defaults (hoje)
    today = dt.date.today().isoformat()

    date_from = request.args.get("dateFrom", today)
    date_to = request.args.get("dateTo", today)
    resources = request.args.get("resources", "MG")

    params = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "resources": resources,
        "q": "status=='notdone'",
        "fields": ",".join(FIELDS),
        "limit": 1000,   # ajuste se quiser
        "offset": 0,
    }

    data = _ofs_get("/activities/", params=params)

    # Normaliza itens
    items = data.get("items", []) or []

    # Pré-serializa JSON seguro pro modal (um por linha)
    # Obs: ensure_ascii=False pra manter acentos
    for it in items:
        it["_json"] = json.dumps(it, ensure_ascii=False)

    return render_template(
        "activities/notdone.html",
        items=items,
        has_more=bool(data.get("hasMore")),
        expression=data.get("expression"),
        date_from=date_from,
        date_to=date_to,
        resources=resources,
    )