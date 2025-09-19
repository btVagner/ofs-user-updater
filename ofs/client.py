# ofs/client.py

import os
import requests
from requests.auth import HTTPBasicAuth

DEFAULT_TIMEOUT = 20


class OFSClient:
    def __init__(self, username=None, password=None):
        self.username = username or os.getenv("OFS_USERNAME")
        self.password = password or os.getenv("OFS_PASSWORD")
        self.auth = HTTPBasicAuth(self.username, self.password)

        # Base principal já usada no projeto
        self.base_url = os.getenv(
            "OFS_BASE_URL",
            "https://verointernet.fs.ocs.oraclecloud.com/rest/ofscCore/v1",
        ).rstrip("/")

        # Base específica para criação (ambiente test) — cai na principal se não definido
        self.base_url_create = os.getenv("OFS_BASE_URL_CREATE", self.base_url).rstrip("/")

    # -------- util interno --------
    def _json_request(self, method, url, json=None, headers=None):
        h = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if headers:
            h.update(headers)
        resp = requests.request(
            method,
            url,
            auth=self.auth,
            json=json,
            headers=h,
            timeout=DEFAULT_TIMEOUT,
        )
        return resp

    # ======== MÉTODOS JÁ UTILIZADOS NO PROJETO ========
    def get_login_by_resource_id(self, resource_id: str) -> str:
        """Retorna o login atrelado a um resource_id via /resources/{id}/users"""
        url = f"{self.base_url}/resources/{resource_id}/users"
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers, auth=self.auth, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()["items"][0]["login"]

    def update_user_type(self, login: str, new_user_type: str):
        """PATCH /users/{login} com {'userType': '...'}"""
        url = f"{self.base_url}/users/{login}"
        payload = {"userType": new_user_type}
        resp = self._json_request("PATCH", url, json=payload)
        resp.raise_for_status()
        return resp.status_code, resp.text

    def authenticated_get(self, url: str) -> dict:
        """GET autenticado genérico que retorna JSON"""
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers, auth=self.auth, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def get_usuarios(self) -> list:
        """Lista todos os usuários paginando de 100 em 100"""
        usuarios = []
        offset = 0
        while True:
            url = f"{self.base_url}/users?limit=100&offset={offset}"
            response = self.authenticated_get(url)
            items = response.get("items") or []
            if not items:
                break
            usuarios.extend(items)
            offset += 100
        return usuarios

    # ======== CRIAÇÃO (RECURSO -> USUÁRIO) ========
    def create_resource(self, id_sap: str, parent_resource_id: str, name: str, email: str):
        """
        PUT /resources/{idSAP}
        Campos fixos:
          - resourceType = "TCV"
          - language     = "br"
          - timeZone     = "(UTC-03:00) Sao Paulo - Brasilia Time (BRT)"
          - status       = "active"
        """
        url = f"{self.base_url_create}/resources/{id_sap}"
        body = {
            "parentResourceId": parent_resource_id,
            "resourceType": "TCV",  # fixo
            "name": name,
            "email": email,
            "language": "br",  # fixo
            "timeZone": "(UTC-03:00) Sao Paulo - Brasilia Time (BRT)",  # fixo
            "status": "active",  # fixo
        }
        # Não dou raise aqui porque 409 pode significar "já existe"
        return self._json_request("PUT", url, json=body)

    def create_user(self, email: str, name: str, id_sap: str, user_type: str, password: str):
        """PUT /users/{email}"""
        url = f"{self.base_url_create}/users/{email}"
        body = {
            "name": name,
            "mainResourceId": id_sap,
            "language": "br",  # fixo
            "timeZone": "(UTC-03:00) Sao Paulo - Brasilia Time (BRT)",  # fixo
            "userType": user_type,
            "password": password,
            "resources": [id_sap],
        }
        # Também não damos raise aqui para permitir tratar 409 externamente
        return self._json_request("PUT", url, json=body)

    def update_resource_deposito(self, id_sap: str, deposito_tecnico: str):
        """
        PATCH /resources/{idSAP}
        Body: {"XR_TEC_DEP": "<depositoTecnico>"}
        Deve ser chamado APÓS o recurso existir (criado ou já existente).
        """
        url = f"{self.base_url_create}/resources/{id_sap}"
        body = {"XR_TEC_DEP": deposito_tecnico}
        return self._json_request("PATCH", url, json=body)
