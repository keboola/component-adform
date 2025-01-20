import json
import logging

import requests
from requests.exceptions import HTTPError
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration


STATE_AUTH_ID = "auth_id"
STATE_REFRESH_TOKEN = "#refresh_token"

ENDPOINT_AUTHORIZE = "https://id.adform.com/sts/connect/authorize"
ENDPOINT_TOKEN = "https://id.adform.com/sts/connect/token"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self._header = None
        self.authorization = None
        self.credentials = None

        params = Configuration(**self.configuration.parameters)
        logging.info(params)

        #  Setup ID: Unique Master Data setup identifier (REQUIRED)
        #  Date To: Upper boundary of the time interval for data retrieval (OPTIONAL)
        #  Days Interval: Time interval for data retrieval in days (REQUIRED)
        #  Hours Interval: Time interval for data retrieval in hours (REQUIRED)
        #  Output Bucket: Name of the bucket in KBC (REQUIRED)
        #  Datasets: List of datasets to retrieve (REQUIRED)
        #  Metadata: List of metadata tables to retrieve (OPTIONAL)
        #  File Charset: Encoding of the returned dataset (OPTIONAL)
        #  Override primary keys: JSON structure of primary keys to override (OPTIONAL)

    def run(self):
        pass

    def _client_init(self):
        self.authorization = self.configuration.config_data["authorization"]
        if not self.authorization.get("oauth_api"):
            raise UserException("For component run, please authenticate.")

        self.credentials = self.authorization["oauth_api"]["credentials"]
        client_id = self.credentials["appKey"]
        client_secret = self.credentials["#appSecret"]

        if not client_id or not client_secret:
            client_id = self.credentials["app_key"]
            client_secret = self.credentials["#app_secret"]

        encrypted_data = json.loads(self.credentials["#data"])

        state_file = self.get_state_file()
        state_file_refresh_token = state_file.get(STATE_REFRESH_TOKEN, [])
        state_file_auth_id = state_file.get(STATE_AUTH_ID, [])

        access_token, refresh_token = self._get_oauth(
            state_file_auth_id, state_file_refresh_token, encrypted_data, client_id, client_secret
        )
        self.write_state_file({
            STATE_AUTH_ID: self.credentials.get("id", ""),
            STATE_REFRESH_TOKEN: refresh_token
        })
        self._header = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

    @staticmethod
    def _get_refresh_token(auth_id, refresh_token, encrypted_data, credentials):
        if not auth_id and refresh_token:
            logging.info("Refresh token loaded from state file")
        elif refresh_token and auth_id == credentials.get("id", ""):
            logging.info("Refresh token loaded from state file")
        else:
            refresh_token = encrypted_data["refresh_token"]
            logging.info("Refresh token loaded from authorization")

        return refresh_token

    def _get_oauth(self, state_file_auth_id, state_file_refresh_token, encrypted_data, client_id, client_secret):
        refresh_token = self._get_refresh_token(
            state_file_auth_id,
            state_file_refresh_token,
            encrypted_data,
            self.credentials
        )
        response = self._request_new_token(client_id, client_secret, refresh_token)
        return response["access_token"], response["refresh_token"]

    def _request_new_token(self, client_id, client_secret, refresh_token):
        try:
            response = requests.post(ENDPOINT_TOKEN, data={
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'scope': 'offline_access'  # is required to get new token via refresh token
            })
            response.raise_for_status()
            return response.json()

        except HTTPError as e:
            raise UserException('Failed to fetch token') from e


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
