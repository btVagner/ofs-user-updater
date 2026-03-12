from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session, jsonify
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database.connection import get_connection
import os
from database.audit import audit_log
from werkzeug.middleware.proxy_fix import ProxyFix
import re
import unicodedata
from routes import register_routes
load_dotenv()

from core.auth import (
    login_required,
    perm_required,
    has_perm,
    any_perm,
    all_perms,
)
app = Flask(__name__)

APP_ROOT = os.getenv("APP_ROOT", "")  # em produção defina "/ofs", em dev deixe vazio
if APP_ROOT:
    app.config["APPLICATION_ROOT"] = APP_ROOT
app.secret_key = os.getenv("FLASK_SECRET_KEY", "minha_chave_secreta")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
register_routes(app)
@app.before_request
def atualizar_online():
    # Só registra se o usuário estiver logado
    if session.get("usuario_logado"):
        try:
            registrar_atividade_usuario()
        except Exception as e:
            # Em produção você pode logar isso
            print(f"[WARN] Falha ao registrar atividade do usuário: {e}")
@app.context_processor
def inject_online_count():
    """Disponibiliza 'usuarios_online_count' em todos os templates."""
    count = 0
    try:
        conn = get_connection()
        cur = conn.cursor()

        limite = datetime.now() - timedelta(minutes=5)  # janela de 5 minutos
        cur.execute(
            "SELECT COUNT(*) FROM usuarios_online WHERE last_seen >= %s",
            (limite,),
        )
        row = cur.fetchone()
        if row:
            count = row[0]

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WARN] Falha ao obter usuarios_online_count: {e}")

    return dict(usuarios_online_count=count)


# Disponibiliza os helpers pra TODOS os templates
app.jinja_env.globals.update(
    has_perm=has_perm,
    any_perm=any_perm,
    all_perms=all_perms,
)


# ==========
# UTILIDADES
# ==========

def get_tipos_user():
    """Carrega lista de tipos do OFS (tabela local tipos_ofs)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"codigo": row[0], "descricao": row[1]} for row in resultados]



def _only_alnum_upper(s: str) -> str:
    s = (s or "").strip().upper()
    # remove acentos
    s_norm = unicodedata.normalize("NFKD", s)
    s_noacc = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    # mantém só letras e números
    s_clean = re.sub(r"[^A-Z0-9]", "", s_noacc)
    return s_clean

def _bairro_variants(nome_bairro_original: str) -> list[str]:
    """
    Regra:
    - Remove acentos
    - Remove espaços
    - Remove caracteres especiais
    - Uppercase
    - Retorna sempre a versão limpa
    - Se houver diferença estrutural relevante, retorna segunda variante
    """

    original = (nome_bairro_original or "").strip()
    if not original:
        return []

    # Versão base (remove acentos e não alfanuméricos)
    base = _only_alnum_upper(original)

    variants = []

    if base:
        variants.append(base)

    # Segunda variante: remove qualquer caractere não ASCII da string original
    # e normaliza novamente
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
    bairro40 = (bairro_key + ("0" * 40))[:40]  # completa e corta em 40
    return f"{id8}{bairro40}"

@app.route("/toquio/td-bucket/inserir-mapeamento-bairro", methods=["GET", "POST"])
@login_required
@perm_required("toquio.td_bucket_insert")
def toquio_td_bucket_inserir_mapeamento_bairro():
    # =========================
    # CONSULTA (GET)
    # =========================
    id_cidade_q_raw = (request.args.get("idCidade") or "").strip()
    nome_cidade_q = (request.args.get("nomeCidade") or "").strip()
    chave_like_q = (request.args.get("chave") or "").strip()

    # Para manter o valor no input do template
    idCidade_q = id_cidade_q_raw

    resultados = []

    # Monta WHERE dinamicamente
    where = []
    params = []

    # idCidade (igualdade)
    if id_cidade_q_raw:
        if not id_cidade_q_raw.isdigit():
            # se preferir, pode retornar mensagem de erro ao invés de ignorar
            # flash("idCidade deve ser numérico", "warning")
            pass
        else:
            where.append("idCidade = %s")
            params.append(int(id_cidade_q_raw))

    # nomeCidade (LIKE)
    if nome_cidade_q:
        where.append("nomeCidade LIKE %s")
        params.append(f"%{nome_cidade_q}%")

    # chave (LIKE)
    if chave_like_q:
        where.append("chave LIKE %s")
        params.append(f"%{chave_like_q}%")

    # Só consulta se tiver pelo menos 1 filtro
    if where:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        sql = """
            SELECT *
            FROM projToquio.td_bucket
        """

        sql += " WHERE " + " AND ".join(where)
        sql += " LIMIT 200"

        cur.execute(sql, params)
        resultados = cur.fetchall() or []

        cur.close()
        conn.close()
    # =========================
    # INSERT (POST)
    # =========================
    if request.method == "POST":

        bucket = (request.form.get("bucket") or "").strip()
        sistema = (request.form.get("sistema") or "").strip().upper()
        nome_cidade = (request.form.get("nomeCidade") or "").strip()
        nome_bairro = (request.form.get("nomeBairro") or "").strip()
        uf = (request.form.get("uf") or "").strip().upper()
        id_cidade_raw = (request.form.get("idCidade") or "").strip()
        area_bucket = (request.form.get("areaBucket") or "").strip()
        filial_cidade_raw = (request.form.get("filialCidade") or "").strip()
        regional_pbi = (request.form.get("Regional_PBI") or "").strip()

        # Normaliza UF
        uf = re.sub(r"[^A-Z]", "", uf)[:3]

        # =========================
        # Validação enxuta
        # =========================
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
            flash(" | ".join(parts), "error")
            return redirect(url_for("toquio_td_bucket_inserir_mapeamento_bairro"))

        id_cidade = int(id_cidade_raw)
        filial_cidade = int(filial_cidade_raw)

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
                "isAtivo": is_ativo
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
                    "isAtivo": is_ativo
                })

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
            cur.close()
            conn.close()

            flash(
                "Inserido com sucesso. "
                + " | ".join([f"Key para o OFS={x['workzone']}" for x in rows_to_insert]),
                "success"
            )

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            flash(f"Erro ao inserir: {str(e)}", "error")

        return redirect(url_for("toquio_td_bucket_inserir_mapeamento_bairro"))

    return render_template(
        "toquio_inserir_mapeamento_bairro.html",
        resultados=resultados,
        idCidade_q=idCidade_q,
        nomeCidade_q=nome_cidade_q,
        chave_q=chave_like_q
    )

# Página Home
@app.route("/")
@login_required
def home():
    # carrega lista de tipos de usuário do OFS (para telas de atualização) 1x por sessão
    if "tipos_user" not in session:
        session["tipos_user"] = get_tipos_user()
    return render_template("home.html")


def registrar_atividade_usuario():
    """Atualiza a tabela usuarios_online com o último acesso do usuário logado."""
    usuario_id = session.get("usuario_id")
    if not usuario_id:
        return

    conn = get_connection()
    cur = conn.cursor()

    agora = datetime.now()
    # se quiser UTC, use datetime.utcnow()

    cur.execute(
        """
        INSERT INTO usuarios_online (usuario_id, last_seen)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)
        """,
        (usuario_id, agora),
    )
    conn.commit()
    cur.close()
    conn.close()

@app.route("/status-online")
@login_required
def status_online():
    conn = get_connection()
    cur = conn.cursor()

    limite = datetime.now() - timedelta(minutes=5)

    cur.execute(
        "SELECT COUNT(*) FROM usuarios_online WHERE last_seen >= %s",
        (limite,),
    )
    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        "usuarios_online": count
    })



if __name__ == "__main__":
    
    app.run(host="0.0.0.0", port=5000)