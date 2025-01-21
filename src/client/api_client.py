from keboola.http_client import HttpClient


BASE_URL = "https://api.adform.com/scope/buyer.masterdata"
MD_FILES_URL_PATH = "/v1/buyer/masterdata/files/"
DOWNLOAD_F_URL_PATH = "/v1/buyer/masterdata/download/"


class APIClient(HttpClient):
    def __init__(self, api_token, md_list_id):
        super().__init__()
        self.api_token = api_token
        self.md_list_id = md_list_id
        self.max_retries = 5
        self.default_http_header = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }

    def retrieve_file_list(self):
        url = f"{BASE_URL}{MD_FILES_URL_PATH}{self.md_list_id}"
        response = self.get(url)
        response.raise_for_status()
        return response.json()

    def download_file(self, file_id, download_path):
        url = f"{BASE_URL}{DOWNLOAD_F_URL_PATH}{self.md_list_id}/{file_id}"
        response = self.get(url, stream=True)
        response.raise_for_status()
        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
