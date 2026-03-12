from flask import render_template, request, redirect, url_for, flash

from database.connection import get_connection
from database.audit import audit_log
from core.auth import login_required, perm_required, current_actor


def init_app(app):

    @app.route("/perfis", methods=["GET", "POST"])
    @login_required
    @perm_required("perfis.gerenciar")
    def perfis_view():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        if request.method == "POST":
            acao = request.form.get("acao")
            perfil_id_raw = (request.form.get("perfil_id") or "").strip()

            if acao == "criar":
                novo_nome = (request.form.get("novo_perfil") or "").strip()
                if not novo_nome:
                    flash("Informe um nome para o novo perfil.", "danger")
                    cur.close()
                    conn.close()
                    return redirect(url_for("perfis_view"))

                slug = novo_nome.lower().strip().replace(" ", "_")

                cur.execute("SELECT COALESCE(MAX(id), 0) + 1 AS prox_id FROM perfis")
                row = cur.fetchone()
                prox_id = row["prox_id"] if row and "prox_id" in row else 1

                cur.execute(
                    "INSERT INTO perfis (id, nome, slug) VALUES (%s, %s, %s)",
                    (prox_id, novo_nome, slug),
                )
                conn.commit()

                actor = current_actor()
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="perfis",
                    action="create",
                    entity_type="perfil",
                    entity_id=prox_id,
                    entity_ref=slug,
                    summary=f"Criou perfil: {novo_nome}",
                    after={"id": prox_id, "nome": novo_nome, "slug": slug},
                )

                flash("Perfil criado com sucesso.", "success")
                cur.close()
                conn.close()
                return redirect(url_for("perfis_view"))

            if acao == "salvar" and perfil_id_raw:
                try:
                    perfil_id = int(perfil_id_raw)
                except ValueError:
                    flash("Perfil inválido.", "danger")
                    cur.close()
                    conn.close()
                    return redirect(url_for("perfis_view"))

                nome_editado = (request.form.get("nome_perfil") or "").strip()
                ids_permissoes = request.form.getlist("permissoes[]")

                if not nome_editado:
                    flash("O nome do perfil não pode ser vazio.", "danger")
                    cur.close()
                    conn.close()
                    return redirect(url_for("perfis_view", perfil_id=perfil_id))

                cur.execute("SELECT id, nome, slug FROM perfis WHERE id = %s", (perfil_id,))
                before_perfil = cur.fetchone()

                cur.execute("""
                    SELECT p.recurso
                    FROM perfil_permissao pp
                    JOIN permissoes p ON p.id = pp.permissao_id
                    WHERE pp.perfil_id = %s
                    ORDER BY p.recurso
                """, (perfil_id,))
                before_perms = [r["recurso"] for r in cur.fetchall()]

                cur.execute(
                    "UPDATE perfis SET nome = %s WHERE id = %s",
                    (nome_editado, perfil_id),
                )

                cur.execute("DELETE FROM perfil_permissao WHERE perfil_id = %s", (perfil_id,))
                for pid in ids_permissoes:
                    try:
                        pid_int = int(pid)
                        cur.execute(
                            "INSERT INTO perfil_permissao (perfil_id, permissao_id) VALUES (%s, %s)",
                            (perfil_id, pid_int),
                        )
                    except ValueError:
                        continue

                conn.commit()

                cur.execute("""
                    SELECT p.recurso
                    FROM perfil_permissao pp
                    JOIN permissoes p ON p.id = pp.permissao_id
                    WHERE pp.perfil_id = %s
                    ORDER BY p.recurso
                """, (perfil_id,))
                after_perms = [r["recurso"] for r in cur.fetchall()]

                actor = current_actor()
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="perfis",
                    action="update",
                    entity_type="perfil",
                    entity_id=perfil_id,
                    entity_ref=(before_perfil or {}).get("slug"),
                    summary=f"Atualizou perfil: {nome_editado}",
                    before={"perfil": before_perfil, "permissoes": before_perms},
                    after={
                        "perfil": {
                            "id": perfil_id,
                            "nome": nome_editado,
                            "slug": (before_perfil or {}).get("slug"),
                        },
                        "permissoes": after_perms,
                    },
                )

                flash("Perfil atualizado com sucesso.", "success")
                cur.close()
                conn.close()
                return redirect(url_for("perfis_view", perfil_id=perfil_id))

            if acao == "deletar" and perfil_id_raw:
                try:
                    perfil_id = int(perfil_id_raw)
                except ValueError:
                    flash("Perfil inválido.", "danger")
                    cur.close()
                    conn.close()
                    return redirect(url_for("perfis_view"))

                cur.execute("SELECT id, nome, slug FROM perfis WHERE id = %s", (perfil_id,))
                perfil_row = cur.fetchone()

                cur.execute(
                    "SELECT COUNT(*) AS total FROM usuarios WHERE tipo_id = %s",
                    (perfil_id,),
                )
                qtd_usuarios = cur.fetchone()["total"]

                if qtd_usuarios > 0:
                    flash(
                        f"Não é possível apagar: existem {qtd_usuarios} usuário(s) usando este perfil.",
                        "danger",
                    )
                    cur.close()
                    conn.close()
                    return redirect(url_for("perfis_view", perfil_id=perfil_id))

                cur.execute("DELETE FROM perfil_permissao WHERE perfil_id = %s", (perfil_id,))
                cur.execute("DELETE FROM perfis WHERE id = %s", (perfil_id,))
                conn.commit()

                actor = current_actor()
                audit_log(
                    actor_user_id=actor.get("id"),
                    actor_username=actor.get("username"),
                    module="perfis",
                    action="delete",
                    entity_type="perfil",
                    entity_id=perfil_id,
                    entity_ref=(perfil_row or {}).get("slug"),
                    summary=f"Removeu perfil: {(perfil_row or {}).get('nome', perfil_id)}",
                    before={"perfil": perfil_row},
                )

                flash("Perfil removido com sucesso.", "success")
                cur.close()
                conn.close()
                return redirect(url_for("perfis_view"))

        cur.execute("SELECT id, nome, slug FROM perfis ORDER BY nome")
        perfis = cur.fetchall()

        perfil_id = request.args.get("perfil_id", type=int)
        if not perfil_id and perfis:
            perfil_id = perfis[0]["id"]

        perfil_atual = None
        if perfil_id:
            for p in perfis:
                if p["id"] == perfil_id:
                    perfil_atual = p
                    break

        cur.execute("SELECT id, recurso, descricao FROM permissoes ORDER BY recurso")
        permissoes = cur.fetchall()

        perfil_permissoes = set()
        user_count = 0

        if perfil_atual:
            cur.execute(
                "SELECT permissao_id FROM perfil_permissao WHERE perfil_id = %s",
                (perfil_atual["id"],),
            )
            perfil_permissoes = {row["permissao_id"] for row in cur.fetchall()}

            cur.execute(
                "SELECT COUNT(*) AS total FROM usuarios WHERE tipo_id = %s",
                (perfil_atual["id"],),
            )
            user_count = cur.fetchone()["total"]

        cur.close()
        conn.close()

        return render_template(
            "perfis.html",
            perfis=perfis,
            perfil_atual=perfil_atual,
            permissoes=permissoes,
            perfil_permissoes=perfil_permissoes,
            user_count=user_count,
        )

    @app.route("/usuarios-por-perfil/<int:perfil_id>")
    @login_required
    @perm_required("perfis.gerenciar")
    def usuarios_por_perfil(perfil_id):
        return redirect(url_for("usuarios_painel", perfil_id=perfil_id))

    @app.route("/usuarios", methods=["GET"])
    @login_required
    @perm_required("usuarios.criar")
    def usuarios_painel():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT id, nome FROM perfis ORDER BY nome")
        perfis = cur.fetchall()

        perfil_id = request.args.get("perfil_id", type=int)

        query = """
            SELECT u.id,
                   u.nome,
                   u.username,
                   p.nome AS perfil_nome,
                   u.last_login
            FROM usuarios u
            LEFT JOIN perfis p ON p.id = u.tipo_id
        """
        params = []
        if perfil_id:
            query += " WHERE u.tipo_id = %s"
            params.append(perfil_id)

        query += " ORDER BY u.nome"

        cur.execute(query, tuple(params))
        usuarios = cur.fetchall()

        cur.close()
        conn.close()

        return render_template(
            "usuarios_painel.html",
            usuarios=usuarios,
            perfis=perfis,
            perfil_id_selecionado=perfil_id,
        )