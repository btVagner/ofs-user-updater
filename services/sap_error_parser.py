import json
import re


_MSG_RE = re.compile(r'"message"\s*:\s*"([^"]+)"')

_SAP_HTTP_SUFFIX_RE = re.compile(
    r'\.The 500 Internal Server Error.*$',
    re.IGNORECASE | re.DOTALL
)

_SAP_CDATA_START_RE = re.compile(r'^\s*<!\[CDATA\[', re.IGNORECASE)
_SAP_CDATA_END_RE = re.compile(r'\]\]>\s*$', re.IGNORECASE)


def _extract_message(val):
    if val is None:
        return None

    s = str(val)
    m = _MSG_RE.search(s)
    if not m:
        return None

    msg = (m.group(1) or "").strip()
    return msg or None


def _normalize_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _strip_sap_wrapper(value):
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    s = _SAP_CDATA_START_RE.sub("", s)
    s = _SAP_CDATA_END_RE.sub("", s)
    s = _SAP_HTTP_SUFFIX_RE.sub("", s)
    s = s.strip()

    return s or None


def _extract_first_json_object(text):
    if not text:
        return None

    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start:i + 1]
                try:
                    return json.loads(candidate)
                except Exception:
                    return None

    return None


def _try_parse_json_string(value):
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_best_text_from_obj(obj):
    if not isinstance(obj, dict):
        return None

    for key in ("data", "Documento", "Resposta", "message"):
        raw = obj.get(key)
        if raw is None:
            continue

        raw_str = str(raw).strip()
        if not raw_str:
            continue

        nested_obj = _try_parse_json_string(raw_str)
        if isinstance(nested_obj, dict):
            nested_best = _extract_best_text_from_obj(nested_obj)
            if nested_best:
                return nested_best

        return raw_str

    return None


SAP_ERROR_RULES = [
    {
        "category": "mac_duplicado",
        "patterns": [
            r"não é possivel inserir mac duplicado",
            r"nao e possivel inserir mac duplicado",
            r"mac duplicado",
        ],
        "message": "MAC duplicado"
    },
    {
        "category": "tecnico_filial_inexistente",
        "patterns": [
            r"o tecnico \d+ nao existe na filial \d+",
        ],
        "message": "Técnico não existe na filial"
    },
    {
        "category": "localizacao_tecnico_inexistente",
        "patterns": [
            r"localizacao do tecnico \d+ nao existe no deposito",
            r"localizacao \d+ no deposito .* nao existe",
        ],
        "message": "Localização do técnico não existe no depósito"
    },
    {
        "category": "warehouse_filial_invalida",
        "patterns": [
            r"warehouse is not assigned to the same branch as the document",
        ],
        "message": "Warehouse não pertence à mesma filial do documento"
    },
    {
        "category": "item_nao_encontrado_warehouse",
        "patterns": [
            r"item .* not found in warehouse",
        ],
        "message": "Item não encontrado no warehouse"
    },
    {
        "category": "bin_sem_saldo",
        "patterns": [
            r"allocated quantity exceeds available quantity",
        ],
        "message": "Quantidade alocada excede o saldo disponível"
    },
    {
        "category": "bin_inativo",
        "patterns": [
            r"inactive bin location",
        ],
        "message": "Bin location inativo"
    },
    {
        "category": "row_without_tax",
        "patterns": [
            r"row without tax was found",
        ],
        "message": "Linha sem imposto"
    },
    {
        "category": "protocolo_duplicado",
        "patterns": [
            r"protocolo adapter já existe no documento",
            r"protocolo já existe no documento",
        ],
        "message": "Protocolo já existe no documento"
    },
    {
        "category": "timeout_integracao",
        "patterns": [
            r"timeoutexception",
            r"read timeout",
        ],
        "message": "Timeout na integração SAP"
    },
    {
        "category": "connection_reset",
        "patterns": [
            r"connection reset",
            r"socketexception",
        ],
        "message": "Falha de conexão com o SAP"
    },
    {
        "category": "erro_generico_sap",
        "patterns": [],
        "message": "Erro genérico no SAP"
    },
]


def parse_sap_error(raw_value, xa_sap_crt):

    if str(xa_sap_crt or "").strip() != "1":
        return {
            "sap_response_message": None,
            "sap_error_category": None,
            "sap_error_raw_extracted": None,
        }

    cleaned = _strip_sap_wrapper(raw_value)

    if not cleaned:
        return {
            "sap_response_message": "Erro SAP sem detalhe",
            "sap_error_category": "erro_sem_detalhe",
            "sap_error_raw_extracted": None,
        }

    obj = _extract_first_json_object(cleaned)

    if isinstance(obj, dict):
        best_text = _extract_best_text_from_obj(obj) or cleaned
    else:
        best_text = cleaned

    best_text = _normalize_space(best_text)

    for rule in SAP_ERROR_RULES:
        for pattern in rule["patterns"]:
            if re.search(pattern, best_text, re.IGNORECASE):
                return {
                    "sap_response_message": rule["message"],
                    "sap_error_category": rule["category"],
                    "sap_error_raw_extracted": best_text,
                }

    return {
        "sap_response_message": "Erro genérico no SAP",
        "sap_error_category": "erro_generico_sap",
        "sap_error_raw_extracted": best_text,
    }