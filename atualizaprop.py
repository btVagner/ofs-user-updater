import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

load_dotenv()

class OFSClient:
    def __init__(self, username, password):
        self.auth = HTTPBasicAuth(username, password)
        self.base_url = "https://verointernet.fs.ocs.oraclecloud.com/rest/ofscCore/v1"

    def get_login_by_resource_id(self, resource_id):
        url = f"{self.base_url}/resources/{resource_id}/users"
        headers = { "Accept": "application/json" }
        response = requests.get(url, headers=headers, auth=self.auth)
        response.raise_for_status()
        return response.json()["items"][0]["login"]

    def update_user_type(self, login, new_user_type):
        url = f"{self.base_url}/users/{login}"
        headers = { "Content-Type": "application/json" }
        payload = { "userType": new_user_type }
        response = requests.patch(url, headers=headers, auth=self.auth, json=payload)
        response.raise_for_status()
        return response.status_code, response.text

if __name__ == "__main__":
    username = os.getenv("OFS_USERNAME")
    password = os.getenv("OFS_PASSWORD")
    novo_user_type = "TEC_NOT_IMP_ALL"

    client = OFSClient(username, password)

    while True:
        id_resource = input("Digite o ID do recurso (ou 'sair' para encerrar): ").strip()
        if id_resource.lower() == 'sair':
            break

        try:
            login = client.get_login_by_resource_id(id_resource)
            status, _ = client.update_user_type(login, novo_user_type)
            print(f"✅ Login: {login} | userType: {novo_user_type} | Status: {status}")
        except Exception as e:
            print(f"❌ Falha ao atualizar o recurso '{id_resource}': {e}")
