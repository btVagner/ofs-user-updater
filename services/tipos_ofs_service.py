from database.connection import get_connection


def get_tipos_user():
    """Carrega lista de tipos do OFS (tabela local tipos_ofs)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT codigo, descricao FROM tipos_ofs ORDER BY descricao")
        resultados = cursor.fetchall()
        return [{"codigo": row[0], "descricao": row[1]} for row in resultados]
    finally:
        cursor.close()
        conn.close()