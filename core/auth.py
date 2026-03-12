from functools import wraps

from flask import session, flash, redirect, url_for

from database.connection import get_connection


def current_actor():
    return {
        "id": session.get("usuario_id"),
        "username": session.get("usuario_logado"),
        "nome": session.get("nome_usuario"),
        "tipo_id": session.get("tipo_id"),
    }


def _carregar_permissoes_por_perfil(perfil_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.recurso
        FROM perfil_permissao pp
        JOIN permissoes p ON p.id = pp.permissao_id
        WHERE pp.perfil_id = %s
    """, (perfil_id,))
    perms = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return perms


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "usuario_logado" not in session:
            flash("Faça login para acessar esta página.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function


def has_perm(recurso: str) -> bool:
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    return recurso in perms


def perm_required(*recursos):
    def deco(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if "usuario_logado" not in session:
                flash("Faça login para acessar esta página.", "danger")
                return redirect(url_for("login"))
            perms = session.get("permissoes", [])
            if not any(r in perms for r in recursos):
                flash("Acesso negado para este recurso.", "danger")
                return redirect(url_for("home"))
            return f(*args, **kwargs)
        return wrapper
    return deco


def any_perm(*recursos) -> bool:
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    perms = set(perms)
    return any(r in perms for r in recursos)


def all_perms(*recursos) -> bool:
    perms = session.get("permissoes")
    if perms is None:
        return session.get("tipo_id") == 1
    perms = set(perms)
    return all(r in perms for r in recursos)