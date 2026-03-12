from flask import render_template, request, redirect, url_for, flash, session
import os
import json
import requests

from database.connection import get_connection
from database.audit import audit_log
from ofs.client import OFSClient
from core.auth import login_required, perm_required, current_actor


def init_app(app):

    @app.route("/fechar-os-adapter", methods=["GET", "POST"])
    @login_required
    @perm_required("adapter.fechar_os")
    def fechar_os_adapter():
        acao = (request.form.get("acao") or "").strip().lower()
        activity_id = (request.form.get("activity_id") or "").strip()

        preview = session.get("adapter_preview")

        def _get_usuario_id_logado():
            uid = session.get("usuario_id")
            if uid:
                return int(uid)

            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT id FROM usuarios WHERE username = %s", (session.get("usuario_logado"),))
            row = cur.fetchone()
            cur.close()
            conn.close()
            return int(row["id"]) if row else 0

        def _log_fechamento(**kwargs):
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO adapter_fechamento_os_log
                (usuario_id, activity_id, resource_id, cod_atendimento, id_fechamento,
                 payload_json, response_status, response_body, error_message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                kwargs.get("usuario_id"),
                kwargs.get("activity_id"),
                kwargs.get("resource_id"),
                kwargs.get("cod_atendimento"),
                kwargs.get("id_fechamento"),
                kwargs.get("payload_json"),
                kwargs.get("response_status"),
                kwargs.get("response_body"),
                kwargs.get("error_message"),
            ))
            conn.commit()
            cur.close()
            conn.close()

        def _to_int_or_keep(v):
            if v is None:
                return None
            s = str(v).strip()
            return int(s) if s.isdigit() else v

        if request.method == "GET":
            session.pop("adapter_preview", None)
            return render_template("fechar_os_adapter.html", stage="form", activity_id="")

        if acao == "preview":
            if not activity_id:
                flash("Informe o ID da atividade OFS.", "danger")
                return render_template("fechar_os_adapter.html", stage="form", activity_id="")

            try:
                client = OFSClient()

                atividade = client.authenticated_get(f"{client.base_url}/activities/{activity_id}")

                resource_id = atividade.get("resourceId")
                if not resource_id:
                    raise ValueError("A atividade não possui resourceId.")

                cod_atendimento = atividade.get("XA_SOL_ID")
                start_time = atividade.get("startTime")
                obs = atividade.get("XA_TSK_NOT")
                id_fechamento = atividade.get("XA_SER_CLO_PRO_ADA") or atividade.get("XA_SER_CLO_IMP_ADA")

                if not cod_atendimento:
                    raise ValueError("Atividade sem XA_SOL_ID (CodAtendimento).")
                if not id_fechamento:
                    raise ValueError("Atividade sem XA_SER_CLO_PRO_ADA e sem XA_SER_CLO_IMP_ADA.")
                if not start_time:
                    raise ValueError("Atividade sem startTime (DataInicioAtendimento).")

                recurso = client.authenticated_get(f"{client.base_url}/resources/{resource_id}")
                resource_name = recurso.get("name")
                xr_user = recurso.get("XR_USER_ADAPTER")
                xr_pass = recurso.get("XR_PASSWORD_ADAPTER")

                if not resource_name:
                    resource_name = "Recurso sem nome"

                if not xr_user or not xr_pass:
                    raise ValueError("Recurso sem XR_USER_ADAPTER ou XR_PASSWORD_ADAPTER.")

                payload = {
                    "usuario": xr_user,
                    "senha": xr_pass,
                    "DadosFechamento": {
                        "CodAtendimento": str(cod_atendimento),
                        "WifiUsuario": "NULL",
                        "WifiSenha": "NULL",
                        "DataInicioAtendimento": str(start_time),
                        "IDFechamento": str(id_fechamento),
                        "MACONU": None,
                        "IDSaidaCaixaEscolhida": None,
                        "IDInterface": None,
                        "JustificativaReagendamento": None,
                        "IDMotivoReagendamento": None,
                        "ObsFechamento": obs,
                        "obsFechamentoLog": "NULL",
                        "CodTecnico": _to_int_or_keep(resource_id),
                        "MovimentouEquipamento": True,
                        "MovimentouMaterial": True,
                        "MovimentouEquipamentoCliente": True
                    }
                }

                preview = {
                    "activity_id": activity_id,
                    "resource_id": str(resource_id),
                    "resource_name": resource_name,
                    "xr_user": xr_user,
                    "xr_pass": xr_pass,
                    "cod_atendimento": str(cod_atendimento),
                    "start_time": str(start_time),
                    "id_fechamento": str(id_fechamento),
                    "obs": obs,
                    "payload": payload,
                }
                session["adapter_preview"] = preview

                return render_template("fechar_os_adapter.html", stage="preview", preview=preview, activity_id=activity_id)

            except Exception as e:
                session.pop("adapter_preview", None)
                flash(f"Erro ao montar preview: {e}", "danger")
                return render_template("fechar_os_adapter.html", stage="form", activity_id=activity_id)

        if acao == "confirmar":
            if not preview:
                flash("Preview expirado. Gere o preview novamente.", "danger")
                return render_template("fechar_os_adapter.html", stage="form", activity_id="")

            try:
                close_url = os.getenv("URL_CLOSE_ADAPTER")
                auth_ada = os.getenv("AUTH_ADA")
                cookie_ada = os.getenv("COOKIE_ADA")

                if not close_url:
                    raise RuntimeError("URL_CLOSE_ADAPTER não configurado no .env.")
                if not auth_ada:
                    raise RuntimeError("AUTH_ADA não configurado no .env.")
                if not cookie_ada:
                    raise RuntimeError("COOKIE_ADA não configurado no .env.")

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Basic {auth_ada}",
                    "Cookie": cookie_ada,
                }

                payload = preview["payload"]
                resp = requests.post(close_url, json=payload, headers=headers, timeout=30)
                try:
                    api_response = resp.json()
                except Exception:
                    api_response = {"raw": (resp.text or "")}

                usuario_id = _get_usuario_id_logado()

                _log_fechamento(
                    usuario_id=usuario_id,
                    activity_id=preview.get("activity_id"),
                    resource_id=preview.get("resource_id"),
                    cod_atendimento=preview.get("cod_atendimento"),
                    id_fechamento=preview.get("id_fechamento"),
                    payload_json=json.dumps(payload, ensure_ascii=False),
                    response_status=resp.status_code,
                    response_body=(resp.text or "")[:65000],
                    error_message=None,
                )

                actor = current_actor()
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="adapter",
                    action="close_os",
                    entity_type="activity",
                    entity_ref=preview.get("activity_id"),
                    summary=f"Fechou OS via Adapter: activityId={preview.get('activity_id')} HTTP={resp.status_code}",
                    meta={
                        "activity_id": preview.get("activity_id"),
                        "resource_id": preview.get("resource_id"),
                        "resource_name": preview.get("resource_name"),
                        "cod_atendimento": preview.get("cod_atendimento"),
                        "id_fechamento": preview.get("id_fechamento"),
                        "status_code": resp.status_code,
                    },
                    api_response=api_response,
                )

                session.pop("adapter_preview", None)

                if 200 <= resp.status_code < 300:
                    flash("Fechamento enviado com sucesso para o Adapter.", "success")
                else:
                    flash(f"Adapter retornou erro HTTP {resp.status_code}.", "danger")

                return render_template(
                    "fechar_os_adapter.html",
                    stage="result",
                    result={"status_code": resp.status_code, "body": (resp.text or "")[:5000]},
                    activity_id=preview.get("activity_id"),
                )

            except Exception as e:
                try:
                    usuario_id = _get_usuario_id_logado()
                    _log_fechamento(
                        usuario_id=usuario_id,
                        activity_id=(preview or {}).get("activity_id") or activity_id,
                        resource_id=(preview or {}).get("resource_id"),
                        cod_atendimento=(preview or {}).get("cod_atendimento"),
                        id_fechamento=(preview or {}).get("id_fechamento"),
                        payload_json=json.dumps((preview or {}).get("payload") or {}, ensure_ascii=False),
                        response_status=None,
                        response_body=None,
                        error_message=str(e),
                    )
                except Exception:
                    pass

                flash(f"Erro ao enviar fechamento: {e}", "danger")
                return render_template(
                    "fechar_os_adapter.html",
                    stage="preview",
                    preview=preview,
                    activity_id=(preview or {}).get("activity_id") or activity_id,
                )

        flash("Ação inválida.", "danger")
        return redirect(url_for("fechar_os_adapter"))