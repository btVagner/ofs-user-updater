import requests
from requests.auth import HTTPBasicAuth
import os

class OFSClient:
    def __init__(self, username=None, password=None):
        self.username = username or os.getenv("OFS_USERNAME")
        self.password = password or os.getenv("OFS_PASSWORD")
        self.auth = HTTPBasicAuth(self.username, self.password)
        self.base_url = "https://verointernet.fs.ocs.oraclecloud.com/rest/ofscCore/v1"

    def get_login_by_resource_id(self, resource_id):
        url = f"{self.base_url}/resources/{resource_id}/users"
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers, auth=self.auth)
        response.raise_for_status()
        return response.json()["items"][0]["login"]

    def update_user_type(self, login, new_user_type):
        url = f"{self.base_url}/users/{login}"
        headers = {"Content-Type": "application/json"}
        payload = {"userType": new_user_type}
        response = requests.patch(url, headers=headers, auth=self.auth, json=payload)
        response.raise_for_status()
        return response.status_code, response.text
