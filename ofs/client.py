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
        """Faz uma request JSON autenticada e retorna o Response (sem raise automático)."""
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

    def get_resources_hierarchy_map(self, progress_callback=None) -> dict:
        """
        Lista recursos do OFS paginando e monta a hierarquia em memória.

        Estrutura gerada por usuário/recurso:
        - Bucket ID
        - Bucket Nome
        - Recurso acima do bucket ID
        - Recurso acima do bucket Nome
        - Segundo nível acima do bucket ID
        - Segundo nível acima do bucket Nome

        Não faz consulta individual por recurso.
        Busca todos os recursos uma vez e cruza tudo em memória.
        """
        resources = {}
        offset = 0
        limit = 100

        fields = ",".join([
            "resourceId",
            "name",
            "parentResourceId",
            "XR_PARENT_RESOURCE",
            "resourceType",
        ])

        loaded = 0

        while True:
            url = f"{self.base_url}/resources?limit={limit}&offset={offset}&fields={fields}"
            response = self.authenticated_get(url)

            items = response.get("items") or []

            if not items:
                break

            for item in items:
                resource_id = (
                    item.get("resourceId")
                    or item.get("id")
                    or item.get("resource_id")
                )

                if not resource_id:
                    continue

                resource_id = str(resource_id)

                resources[resource_id] = {
                    "resourceId": resource_id,
                    "name": item.get("name") or "-",
                    "parentResourceId": item.get("parentResourceId") or "-",
                    "XR_PARENT_RESOURCE": item.get("XR_PARENT_RESOURCE") or "-",
                    "resourceType": item.get("resourceType") or "-",
                }

            loaded += len(items)

            if progress_callback:
                progress_callback(loaded)

            offset += limit

        bucket_by_resource = {}
        bucket_name_by_resource = {}

        bucket_parent_by_resource = {}
        bucket_parent_name_by_resource = {}

        bucket_grandparent_by_resource = {}
        bucket_grandparent_name_by_resource = {}

        for resource_id, resource in resources.items():
            bucket_id = (
                resource.get("XR_PARENT_RESOURCE")
                if resource.get("XR_PARENT_RESOURCE") not in (None, "", "-")
                else resource.get("parentResourceId")
            )

            if not bucket_id or bucket_id == "-":
                bucket_by_resource[resource_id] = "-"
                bucket_name_by_resource[resource_id] = "-"

                bucket_parent_by_resource[resource_id] = "-"
                bucket_parent_name_by_resource[resource_id] = "-"

                bucket_grandparent_by_resource[resource_id] = "-"
                bucket_grandparent_name_by_resource[resource_id] = "-"
                continue

            bucket_id = str(bucket_id)
            bucket = resources.get(bucket_id, {})

            bucket_parent_id = bucket.get("parentResourceId") or "-"
            bucket_parent = (
                resources.get(str(bucket_parent_id), {})
                if bucket_parent_id and bucket_parent_id != "-"
                else {}
            )

            bucket_grandparent_id = bucket_parent.get("parentResourceId") or "-"
            bucket_grandparent = (
                resources.get(str(bucket_grandparent_id), {})
                if bucket_grandparent_id and bucket_grandparent_id != "-"
                else {}
            )

            bucket_by_resource[resource_id] = bucket_id
            bucket_name_by_resource[resource_id] = bucket.get("name") or "-"

            bucket_parent_by_resource[resource_id] = bucket_parent_id
            bucket_parent_name_by_resource[resource_id] = bucket_parent.get("name") or "-"

            bucket_grandparent_by_resource[resource_id] = bucket_grandparent_id
            bucket_grandparent_name_by_resource[resource_id] = bucket_grandparent.get("name") or "-"

        return {
            "resources": resources,
            "bucket_by_resource": bucket_by_resource,
            "bucket_name_by_resource": bucket_name_by_resource,
            "bucket_parent_by_resource": bucket_parent_by_resource,
            "bucket_parent_name_by_resource": bucket_parent_name_by_resource,
            "bucket_grandparent_by_resource": bucket_grandparent_by_resource,
            "bucket_grandparent_name_by_resource": bucket_grandparent_name_by_resource,
        }
    # ======== MÉTODOS JÁ UTILIZADOS NO PROJETO ========
    def authenticated_get(self, url: str) -> dict:
        """GET autenticado genérico que retorna JSON (com raise em erro HTTP)."""
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers, auth=self.auth, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def get_login_by_resource_id(self, resource_id: str) -> str:
        """Retorna o login atrelado a um resource_id via /resources/{id}/users"""
        url = f"{self.base_url}/resources/{resource_id}/users"
        data = self.authenticated_get(url)
        return data["items"][0]["login"]

    def update_user_type(self, login: str, new_user_type: str):
        """PATCH /users/{login} com {'userType': '...'}"""
        url = f"{self.base_url}/users/{login}"
        payload = {"userType": new_user_type}
        resp = self._json_request("PATCH", url, json=payload)
        resp.raise_for_status()
        return resp.status_code, resp.text

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

    # ======== Bucket (XR_PARENT_RESOURCE) ========
    def get_bucket_by_resource_id(self, resource_id: str) -> str:
        """
        GET /resources/{resourceId}
        Retorna o valor de XR_PARENT_RESOURCE (Bucket) se existir, senão '-'.
        """
        if not resource_id:
            return "-"
        url = f"{self.base_url}/resources/{resource_id}"
        data = self.authenticated_get(url)
        return data.get("XR_PARENT_RESOURCE") or "-"



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
        # Não fazemos raise aqui (para permitir tratar 409 externamente)
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
        # Não fazemos raise; quem chama decide como tratar códigos de retorno
        return self._json_request("PATCH", url, json=body)
