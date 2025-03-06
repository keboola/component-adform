import os
import gzip
import zipfile
import json
import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import requests
import backoff
import duckdb
from duckdb.duckdb import DuckDBPyConnection
from keboola.component.base import ComponentBase
from keboola.component.dao import SupportedDataTypes, BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from requests.exceptions import HTTPError

from configuration import Configuration
from client.api_client import AdformClient

DUCK_DB_MAX_MEMORY = "400MB"
DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")
FILES_TEMP_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "files")

STATE_AUTH_ID = "auth_id"
STATE_REFRESH_TOKEN = "#refresh_token"

ENDPOINT_AUTHORIZE = "https://id.adform.com/sts/connect/authorize"
ENDPOINT_TOKEN = "https://id.adform.com/sts/connect/token"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.token = self._get_access_token()
        self.duck = self.init_duckdb()
        os.makedirs(os.path.dirname(FILES_TEMP_DIR), exist_ok=True)
        os.makedirs(FILES_TEMP_DIR, exist_ok=True)

    def run(self):
        params = Configuration(**self.configuration.parameters)

        setup_id = params.source.setup_id
        days_interval = params.source.days_interval
        hours_interval = params.source.hours_interval
        datasets = params.source.datasets

        date_to = params.source.date_to
        custom_pkeys = params.destination.override_pkey
        incremental = params.destination.incremental
        file_charset = params.source.file_charset
        meta_files = params.source.meta_files

        client = AdformClient(self.token, setup_id)

        try:
            files = client.retrieve_file_list()

        except requests.exceptions.RequestException as e:
            raise UserException(f"Failed to retrieve file list: {str(e)}")

        start_interval, end_interval = self._calculate_start_interval(date_to, days_interval, hours_interval)
        filtered_files = self.filter_files_by_date_and_dataset(files, start_interval, end_interval, datasets)

        for file in filtered_files:
            try:
                client.download_file(file, FILES_TEMP_DIR)
            except requests.exceptions.RequestException as e:
                raise UserException(f"Failed to download file: {str(e)}")

        for prefix in datasets:
            downloaded_files = [f for f in filtered_files if f["name"].startswith(prefix)]
            if downloaded_files:
                logging.info(f"Processing dataset: {prefix}")
                self.save_to_table(prefix, downloaded_files, file_charset, custom_pkeys, incremental)

        if meta_files:
            client.download_file({"id": "meta__zip", "name": "meta.zip", "setup": setup_id}, FILES_TEMP_DIR)
            self.unzip_file(f"{FILES_TEMP_DIR}/meta.zip", os.path.join(FILES_TEMP_DIR, "meta"))
            for dim in meta_files:
                logging.info(f"Processing meta file: {dim}")
                self.save_metadata_to_table(dim)
        print("Component finished successfully")

    def save_to_table(self, prefix, downloaded_files, file_charset, custom_pkeys, incremental):
        if file_charset == "UTF-8":  # if using UTF-8 we can load directly to DuckDB which handles gzip
            self.duck.execute(f"""
                CREATE VIEW {prefix} AS
                SELECT * FROM read_csv(
                    '{FILES_TEMP_DIR}/{prefix}_*.csv.gz',
                    union_by_name=true,
                    types={
                        'VisibilityTime': 'BIGINT',
                        'MouseOvers': 'BIGINT',
                        'MouseOverTime': 'BIGINT'
                    }
                )
            """)

        else:  # if not UTF-8 we need to first ungzip and convert to UTF-8 and then pass to DuckDB
            to_process = [f["name"] for f in downloaded_files if f["name"].startswith(prefix)]
            unzipped = self.ungzip_convert_to_utf8(to_process, file_charset, FILES_TEMP_DIR)
            self.duck.execute(f"""
                CREATE VIEW {prefix} AS
                SELECT * FROM read_csv(
                    '{unzipped}/{prefix}_*.csv.gz',
                    union_by_name=true,
                    types={
                        'VisibilityTime': 'BIGINT',
                        'MouseOvers': 'BIGINT',
                        'MouseOverTime': 'BIGINT'
                    }
                )
            """)

        table_meta = self.duck.execute(f"""DESCRIBE {prefix};""").fetchall()
        schema = OrderedDict(
            {c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))) for c in table_meta}
        )

        primary_key = None
        if custom_pkeys:
            primary_key = [key for item in custom_pkeys if item.dataset == prefix for key in item.pkey]
        elif schema.get("GUID"):
            primary_key = ["GUID"]

        out_table = self.create_out_table_definition(
            f"{prefix}.csv", schema=schema, primary_key=primary_key, incremental=incremental, has_header=True
        )

        try:
            self.duck.execute(f"COPY {prefix} TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)")
        except duckdb.duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

        self.write_manifest(out_table)

    def save_metadata_to_table(self, dim):
        try:
            view_name = dim.replace("-", "_")
            self.duck.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{FILES_TEMP_DIR}/meta/{dim}.json'")

            table_meta = self.duck.execute(f"""DESCRIBE {view_name};""").fetchall()
            schema = OrderedDict(
                {c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))) for c in table_meta}
            )

            primary_key = ["id"] if schema.get("id") else None

            out_table = self.create_out_table_definition(
                f"meta-{dim}.csv", schema=schema, primary_key=primary_key, has_header=True
            )

            self.duck.execute(f"COPY {view_name} TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)")

            self.write_manifest(out_table)

        except duckdb.duckdb.IOException as e:
            logging.error(f"Metadata file not found: {e}")
        except Exception as e:
            raise UserException(f"Error during processing metadata file: {e}")

    @staticmethod
    def ungzip_convert_to_utf8(in_files: list[str], source_encoding: str, input_dir: str) -> str:
        ungzipped_dir = os.path.join(input_dir, "unzipped")
        os.makedirs(ungzipped_dir, exist_ok=True)

        for filename in in_files:
            input_path = os.path.join(input_dir, filename)
            output_filename = filename[:-3] if filename.endswith(".gz") else filename
            output_path = os.path.join(ungzipped_dir, output_filename)

            if filename.endswith(".gz"):
                with (
                    gzip.open(input_path, "rt", encoding=source_encoding) as f_in,
                    open(output_path, "w", encoding="utf-8") as f_out,
                ):
                    while True:
                        chunk = f_in.read(1024 * 1024)
                        if not chunk:
                            break
                        f_out.write(chunk)

        return ungzipped_dir

    @staticmethod
    def unzip_file(zip_path, extract_path):
        with zipfile.ZipFile(zip_path) as zip_ref:
            zip_ref.extractall(extract_path)

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

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
        self.save_new_token(refresh_token)
        return access_token

    def _get_oauth(self, state_file_auth_id, state_file_refresh_token, encrypted_data, client_id, client_secret):
        refresh_token = self._get_refresh_token(
            state_file_auth_id, state_file_refresh_token, encrypted_data, self.credentials
        )
        response = self._request_new_token(client_id, client_secret, refresh_token)
        return response["access_token"], response["refresh_token"]

    def _request_new_token(self, client_id, client_secret, refresh_token):
        try:
            response = requests.post(
                ENDPOINT_TOKEN,
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "scope": "offline_access",  # is required to get new token via refresh token
                },
            )
            response.raise_for_status()
            return response.json()

        except HTTPError as e:
            raise UserException(f"Failed to fetch token {str(e)}") from e

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

    def save_new_token(self, refresh_token: str) -> None:
        """
        The refresh token is invalidated after each use, so we need to save the new one.

        We need to have two ways of saving the token:
        - To data/out/state.json via the component library (used by KBC when the job is successful)
        - Via the Storage API (for cases when the job fails)

        :param refresh_token: The new refresh token to be saved
        :return: None
        """
        self.write_state_file({STATE_AUTH_ID: self.credentials.get("id", ""), STATE_REFRESH_TOKEN: refresh_token})
        if self.environment_variables.stack_id:
            logging.debug("Saving new refresh token to state using Keboola API.")
            try:
                encrypted_refresh_token = self.encrypt(refresh_token)
            except requests.exceptions.RequestException:
                logging.warning("Encrypt API is unavailable. Skipping token save at the beginning of the run.")
                return

            new_state = {
                "component": {
                    STATE_AUTH_ID: self.credentials.get("id", ""),
                    STATE_REFRESH_TOKEN: encrypted_refresh_token
                }
            }
            try:
                self.update_config_state_api(
                    component_id=self.environment_variables.component_id,
                    configurationId=self.environment_variables.config_id,
                    state=new_state,
                    branch_id=self.environment_variables.branch_id,
                )
            except requests.exceptions.RequestException:
                logging.warning(
                    "Storage API (update config state)is unavailable. Skipping token save at the beginning of the run."
                )
                return

    def _get_storage_token(self) -> str:
        token = self.configuration.parameters.get("#storage_token") or self.environment_variables.token
        if not token:
            raise UserException("Cannot retrieve storage token from env variables and/or config.")
        return token

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        url = "https://encryption.keboola.com/encrypt"
        params = {
            "componentId": self.environment_variables.component_id,
            "projectId": self.environment_variables.project_id,
            "configId": self.environment_variables.config_id,
        }
        headers = {"Content-Type": "text/plain"}

        response = requests.post(url, data=token, params=params, headers=headers)
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def update_config_state_api(self, component_id, configurationId, state, branch_id="default"):
        if not branch_id:
            branch_id = "default"

        region = os.environ.get("KBC_STACKID")

        url = (
            f"https://{region}/v2/storage/branch/{branch_id}/components/{component_id}/configs/{configurationId}/state"
        )

        parameters = {"state": json.dumps(state)}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "X-StorageApi-Token": self._get_storage_token()}
        response = requests.put(url, data=parameters, headers=headers)
        response.raise_for_status()

    @staticmethod
    def init_duckdb() -> DuckDBPyConnection:
        """
        Returns connection to temporary DuckDB database
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(temp_directory=DUCK_DB_DIR, threads="1", max_memory=DUCK_DB_MAX_MEMORY)
        conn = duckdb.connect(config=config)

        return conn

    @staticmethod
    def _calculate_start_interval(date_to=None, days_interval=0, hours_interval=0):
        if date_to:
            end_date = datetime.strptime(date_to, "%d-%m-%Y %H:%M").replace(tzinfo=timezone.utc)
        else:
            end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_interval, hours=hours_interval)
        return start_date, end_date

    @staticmethod
    def filter_files_by_date_and_dataset(files, start_date, end_date, datasets):
        filtered_files = []
        for file in files:
            file_date = datetime.strptime(file["createdAt"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (end_date is None and start_date <= file_date) or (start_date <= file_date <= end_date):
                if any(file["name"].startswith(prefix) for prefix in datasets):
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
