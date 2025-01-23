from collections.abc import Iterator
from typing import Dict
from keboola.http_client import HttpClient
import os

BASE_URL = "https://api.adform.com/"
MD_FILES_URL_PATH = "/v1/buyer/masterdata/files/"
DOWNLOAD_F_URL_PATH = "/v1/buyer/masterdata/download/"

PAGE_SIZE = 1000


class AdformClient(HttpClient):
    def __init__(self, api_token, setup_id):
        super().__init__(BASE_URL)
        self.update_auth_header({"Authorization": f'Bearer {api_token}'})
        self.setup_id = setup_id

    def retrieve_file_list(self) -> Iterator[Dict]:
        offset = 0
        while True:
            endpoint = f"{MD_FILES_URL_PATH}{self.setup_id}"
            params = {
                "limit": PAGE_SIZE,
                "offset": offset
            }
            headers = {"Return-Total-Count": "true"}
            
            response = self.get(endpoint, params=params, headers=headers)

            data = response
            if not data:  # No more results
                break
                
            for item in data:
                yield item
                
            if len(data) < PAGE_SIZE:  # Last page
                break
                
            offset += PAGE_SIZE

    def download_file(self, file_dict, dir_path):
        endpoint = f"{DOWNLOAD_F_URL_PATH}{file_dict['setup']}/{file_dict['id']}"
        response = self.get_raw(endpoint, stream=True)
        response.raise_for_status()
        
        dir_path = os.path.abspath(dir_path)
        full_path = os.path.join(dir_path, file_dict['name'])
            
        with open(full_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
