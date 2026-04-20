from datetime import datetime, timedelta

from database.connection import get_connection


ONLINE_WINDOW_MINUTES = 5


def registrar_atividade_usuario(usuario_id):
    """Atualiza a tabela usuarios_online com o último acesso do usuário logado."""
    if not usuario_id:
        return

    conn = get_connection()
    cur = conn.cursor()

    try:
        agora = datetime.now()

        cur.execute(
            """
            INSERT INTO usuarios_online (usuario_id, last_seen)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)
            """,
            (usuario_id, agora),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def obter_usuarios_online_count():
    conn = get_connection()
    cur = conn.cursor()

    try:
        limite = datetime.now() - timedelta(minutes=ONLINE_WINDOW_MINUTES)
        cur.execute(
            "SELECT COUNT(*) FROM usuarios_online WHERE last_seen >= %s",
            (limite,),
        )
        row = cur.fetchone()
        return row[0] if row else 0
    finally:
        cur.close()
        conn.close()