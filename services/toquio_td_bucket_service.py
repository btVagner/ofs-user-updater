import re
import unicodedata

from database.connection import get_connection


def _only_alnum_upper(s: str) -> str:
    s = (s or "").strip().upper()
    s_norm = unicodedata.normalize("NFKD", s)
    s_noacc = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    s_clean = re.sub(r"[^A-Z0-9]", "", s_noacc)
    return s_clean


def _bairro_variants(nome_bairro_original: str) -> list[str]:
    original = (nome_bairro_original or "").strip()
    if not original:
        return []

    base = _only_alnum_upper(original)
    variants = []

    if base:
        variants.append(base)

    ascii_strict = (
        unicodedata.normalize("NFKD", original)
        .encode("ASCII", "ignore")
        .decode("ASCII")
    )
    ascii_strict = re.sub(r"[^A-Za-z0-9]", "", ascii_strict).upper()

    if ascii_strict and ascii_strict not in variants:
        variants.append(ascii_strict)

    return variants


def _workzone(id_cidade_int: int, bairro_key: str) -> str:
    """
    workzone = 8 dígitos do idCidade com zero à esquerda + (bairro_key + zeros) completando 40 chars.
    Total 48.
    """
    id8 = str(int(id_cidade_int)).zfill(8)
    bairro_key = (bairro_key or "").strip().upper()
    bairro40 = (bairro_key + ("0" * 40))[:40]
    return f"{id8}{bairro40}"


def consultar_td_bucket(id_cidade_q_raw: str, nome_cidade_q: str, chave_like_q: str):
    resultados = []

    where = []
    params = []

    if id_cidade_q_raw:
        if id_cidade_q_raw.isdigit():
            where.append("idCidade = %s")
            params.append(int(id_cidade_q_raw))

    if nome_cidade_q:
        where.append("nomeCidade LIKE %s")
        params.append(f"%{nome_cidade_q}%")

    if chave_like_q:
        where.append("chave LIKE %s")
        params.append(f"%{chave_like_q}%")

    if not where:
        return resultados

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        sql = """
            SELECT *
            FROM projToquio.td_bucket
        """
        sql += " WHERE " + " AND ".join(where)
        sql += " LIMIT 200"

        cur.execute(sql, params)
        resultados = cur.fetchall() or []
        return resultados
    finally:
        cur.close()
        conn.close()


def validar_payload_td_bucket(form):
    bucket = (form.get("bucket") or "").strip()
    sistema = (form.get("sistema") or "").strip().upper()
    nome_cidade = (form.get("nomeCidade") or "").strip()
    nome_bairro = (form.get("nomeBairro") or "").strip()
    uf = (form.get("uf") or "").strip().upper()
    id_cidade_raw = (form.get("idCidade") or "").strip()
    area_bucket = (form.get("areaBucket") or "").strip()
    filial_cidade_raw = (form.get("filialCidade") or "").strip()
    regional_pbi = (form.get("Regional_PBI") or "").strip()

    uf = re.sub(r"[^A-Z]", "", uf)[:3]

    required_fields = {
        "bucket": "Bucket",
        "sistema": "Sistema de origem",
        "nomeCidade": "Nome da cidade",
        "uf": "UF",
        "idCidade": "idCidade",
        "areaBucket": "Região",
        "filialCidade": "filialCidade",
        "Regional_PBI": "Regional_PBI",
    }

    values = {
        "bucket": bucket,
        "sistema": sistema,
        "nomeCidade": nome_cidade,
        "uf": uf,
        "idCidade": id_cidade_raw,
        "areaBucket": area_bucket,
        "filialCidade": filial_cidade_raw,
        "Regional_PBI": regional_pbi,
    }

    missing = [label for key, label in required_fields.items() if not values.get(key)]

    invalid_numeric = []
    if id_cidade_raw and not id_cidade_raw.isdigit():
        invalid_numeric.append("idCidade")
    if filial_cidade_raw and not filial_cidade_raw.isdigit():
        invalid_numeric.append("filialCidade")

    if missing or invalid_numeric:
        parts = []
        if missing:
            parts.append("Campos obrigatórios vazios: " + ", ".join(missing))
        if invalid_numeric:
            parts.append("Campos numéricos inválidos: " + ", ".join(invalid_numeric))
        return {
            "ok": False,
            "message": " | ".join(parts),
            "data": None,
        }

    return {
        "ok": True,
        "message": "",
        "data": {
            "bucket": bucket,
            "sistema": sistema,
            "nome_cidade": nome_cidade,
            "nome_bairro": nome_bairro,
            "uf": uf,
            "id_cidade": int(id_cidade_raw),
            "area_bucket": area_bucket,
            "filial_cidade": int(filial_cidade_raw),
            "regional_pbi": regional_pbi,
        },
    }


def montar_rows_td_bucket(data: dict):
    bucket = data["bucket"]
    sistema = data["sistema"]
    nome_cidade = data["nome_cidade"]
    nome_bairro = data["nome_bairro"]
    uf = data["uf"]
    id_cidade = data["id_cidade"]
    area_bucket = data["area_bucket"]
    filial_cidade = data["filial_cidade"]
    regional_pbi = data["regional_pbi"]

    supervisor = "DEFAULT"
    is_ativo = 1
    id_bairro = None

    variants = _bairro_variants(nome_bairro)
    rows_to_insert = []

    if not variants:
        bairro_key = "0000000"
        chave = f"{sistema}{id_cidade}DEFAULT"
        workzone = _workzone(id_cidade, bairro_key)

        rows_to_insert.append({
            "bucket": bucket,
            "chave": chave,
            "idCidade": id_cidade,
            "idBairro": id_bairro,
            "nomeCidade": nome_cidade,
            "nomeBairro": nome_bairro,
            "uf": uf,
            "workzone": workzone,
            "supervisor": supervisor,
            "sistema": sistema,
            "areaBucket": area_bucket,
            "filialCidade": filial_cidade,
            "Regional_PBI": regional_pbi,
            "isAtivo": is_ativo,
        })
    else:
        for bairro_key in variants:
            chave = f"{id_cidade}{bairro_key}"
            workzone = _workzone(id_cidade, bairro_key)

            rows_to_insert.append({
                "bucket": bucket,
                "chave": chave,
                "idCidade": id_cidade,
                "idBairro": id_bairro,
                "nomeCidade": nome_cidade,
                "nomeBairro": nome_bairro,
                "uf": uf,
                "workzone": workzone,
                "supervisor": supervisor,
                "sistema": sistema,
                "areaBucket": area_bucket,
                "filialCidade": filial_cidade,
                "Regional_PBI": regional_pbi,
                "isAtivo": is_ativo,
            })

    return rows_to_insert


def inserir_td_bucket(rows_to_insert: list[dict]):
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        sql_insert = """
            INSERT INTO projToquio.td_bucket
            (bucket, chave, idCidade, idBairro, nomeCidade, nomeBairro, uf, workzone,
             supervisor, sistema, areaBucket, filialCidade, Regional_PBI, isAtivo)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for r in rows_to_insert:
            cur.execute(sql_insert, (
                r["bucket"], r["chave"], r["idCidade"], r["idBairro"],
                r["nomeCidade"], r["nomeBairro"], r["uf"], r["workzone"],
                r["supervisor"], r["sistema"], r["areaBucket"],
                r["filialCidade"], r["Regional_PBI"], r["isAtivo"]
            ))

        conn.commit()
        return {
            "ok": True,
            "message": (
                "Inserido com sucesso. "
                + " | ".join([f"Key para o OFS={x['workzone']}" for x in rows_to_insert])
            ),
        }

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "ok": False,
            "message": f"Erro ao inserir: {str(e)}",
        }

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()