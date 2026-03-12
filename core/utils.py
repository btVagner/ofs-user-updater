from openpyxl.utils import get_column_letter


def xlsx_auto_width(ws, max_width=60):
    """
    Ajuste simples de largura de colunas para planilhas XLSX.
    """
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)

        for cell in col:
            try:
                v = "" if cell.value is None else str(cell.value)
                if len(v) > max_len:
                    max_len = len(v)
            except Exception:
                pass

        ws.column_dimensions[col_letter].width = min(max_len + 2, max_width)