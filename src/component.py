import json
import logging
from datetime import datetime, timedelta, timezone

import requests
from requests.exceptions import HTTPError
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client.api_client import APIClient
from configuration import Configuration
from file import FileHandler


STATE_AUTH_ID = "auth_id"
STATE_REFRESH_TOKEN = "#refresh_token"

ENDPOINT_AUTHORIZE = "https://id.adform.com/sts/connect/authorize"
ENDPOINT_TOKEN = "https://id.adform.com/sts/connect/token"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        #  Setup ID: Unique Master Data setup identifier (REQUIRED)
        #  Date To: Upper boundary of the time interval for data retrieval (OPTIONAL)
        #  Days Interval: Time interval for data retrieval in days (REQUIRED)
        #  Hours Interval: Time interval for data retrieval in hours (REQUIRED)
        #  Output Bucket: Name of the bucket in KBC (REQUIRED)
        #  Prefixes: List of datasets to retrieve (REQUIRED)
        #  Metadata: List of metadata tables to retrieve (OPTIONAL)
        #  File Charset: Encoding of the returned dataset (OPTIONAL)
        #  Override primary keys: JSON structure of primary keys to override (OPTIONAL)

    def run(self):
        token = self._get_access_token()
        params = Configuration(**self.configuration.parameters)
        setupId = params.requiredParameters.setupId
        outputBucket = params.requiredParameters.outputBucket  # noqaF841
        daysInterval = params.requiredParameters.daysInterval
        hoursInterval = params.requiredParameters.hoursInterval
        prefixes = params.requiredParameters.prefixes

        dateTo = params.optionalParameters.dateTo
        custom_pkeys = params.optionalParameters.override_pkey  # noqaF841
        fileCharset = params.optionalParameters.fileCharset
        metaFiles = params.optionalParameters.metaFiles  # noqaF841

        client = APIClient(token, setupId)
        fileHandler = FileHandler()

        try:
            files = client.retrieve_file_list()

        except requests.exceptions.RequestException as e:
            raise UserException(f"Failed to retrieve file list: {str(e)}")

        start_interval, end_interval = self._calculate_start_interval(dateTo, daysInterval, hoursInterval)
        filtered_files = self.filter_files_by_date_and_prefix(files, start_interval, end_interval, prefixes)

        for file in filtered_files:
            file_id = file['id']
            file_name = file['name']
            download_path = f"/data/in/files/{file_name}"
            try:
                client.download_file(file_id, download_path)
            except requests.exceptions.RequestException as e:
                raise UserException(f"Failed to download file: {str(e)}")

        for prefix in prefixes:
            downloaded_files = [f for f in filtered_files if f['name'].startswith(prefix)]
            if downloaded_files:
                master_files = fileHandler.unzip_files(downloaded_files, '/data/in/files')
                fileHandler.merge_files(master_files, '/data/out/tables', f"{prefix}.csv", fileCharset)

    def _get_access_token(self):
        self.authorization = self.configuration.config_data["authorization"]
        if not self.authorization.get("oauth_api"):
            raise UserException("For component run, please authenticate.")

        self.credentials = self.authorization["oauth_api"]["credentials"]
        client_id = self.credentials["appKey"]
        client_secret = self.credentials["#appSecret"]
        encrypted_data = json.loads(self.credentials["#data"])

        if not client_id or not client_secret:
            client_id = self.credentials["app_key"]
            client_secret = self.credentials["#app_secret"]

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

        return access_token

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

    @staticmethod
    def _calculate_start_interval(date_to=None, days_interval=0, hours_interval=0):
        if date_to:
            end_date = datetime.strptime(date_to, "%d-%m-%Y %H:%M").replace(tzinfo=timezone.utc)
        else:
            end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_interval, hours=hours_interval)
        return start_date, end_date

    @staticmethod
    def filter_files_by_date_and_prefix(files, start_date, end_date, prefixes):
        filtered_files = []
        for file in files:
            file_date = datetime.strptime(file['createdAt'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (end_date is None and start_date <= file_date) or (start_date <= file_date <= end_date):
                if any(file['name'].startswith(prefix) for prefix in prefixes):
                    filtered_files.append(file)
        return filtered_files


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
