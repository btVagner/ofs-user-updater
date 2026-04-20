from flask import render_template, request, redirect, url_for, flash

from core.auth import login_required, perm_required
from services.toquio_td_bucket_service import (
    consultar_td_bucket,
    validar_payload_td_bucket,
    montar_rows_td_bucket,
    inserir_td_bucket,
)


def init_app(app):
    @app.route("/toquio/td-bucket/inserir-mapeamento-bairro", methods=["GET", "POST"])
    @login_required
    @perm_required("toquio.td_bucket_insert")
    def toquio_td_bucket_inserir_mapeamento_bairro():
        id_cidade_q_raw = (request.args.get("idCidade") or "").strip()
        nome_cidade_q = (request.args.get("nomeCidade") or "").strip()
        chave_like_q = (request.args.get("chave") or "").strip()

        idCidade_q = id_cidade_q_raw
        resultados = consultar_td_bucket(id_cidade_q_raw, nome_cidade_q, chave_like_q)

        if request.method == "POST":
            validation = validar_payload_td_bucket(request.form)

            if not validation["ok"]:
                flash(validation["message"], "error")
                return redirect(url_for("toquio_td_bucket_inserir_mapeamento_bairro"))

            rows_to_insert = montar_rows_td_bucket(validation["data"])
            result = inserir_td_bucket(rows_to_insert)

            flash(result["message"], "success" if result["ok"] else "error")
            return redirect(url_for("toquio_td_bucket_inserir_mapeamento_bairro"))

        return render_template(
            "toquio_inserir_mapeamento_bairro.html",
            resultados=resultados,
            idCidade_q=idCidade_q,
            nomeCidade_q=nome_cidade_q,
            chave_q=chave_like_q
        )