import os
import logging
import duckdb
from dotenv import load_dotenv
from google.cloud import storage
from settings import TABLES_CONFIG
from utils import upload_to_gcs, parquet_to_bq

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
BUCKET_ID = os.getenv("BUCKET_ID")
GCS_PREFIX = os.getenv("GCS_PREFIX")

class ExtractDataGCS:

    def __init__(self, project_id: str, bucket_id: str):
        self.gcs_client = None
        self.project_id = project_id
        self.bucket_id = bucket_id

    def _get_gcs_client(self):
        if not self.gcs_client:
            try:
                self.gcs_client = storage.Client(self.project_id)
            except Exception as e:
                logging.error('Could not create gcs client %s', e)
                raise
        return self.gcs_client

    def download_files(
        self, gcs_prefix: str, file_format: str, local_path: str = None) -> list[str]:
        gcs_client = self._get_gcs_client()
        gcs_bucket = gcs_client.bucket(self.bucket_id)
        local_path = local_path or 'downloads/'

        files = []
        for blob in gcs_bucket.list_blobs(prefix=gcs_prefix):
            if blob.name.endswith(file_format):
                files.append(blob.name)

        try:
            downloads_path = []
            # can be changed to parallel download if necessary
            for blob_name in files:
                blob = gcs_bucket.blob(blob_name)

                path = os.path.join(local_path, blob.name)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                blob.download_to_filename(path)

                downloads_path.append(path)
        except Exception as e:
            logging.error('Could not download all files from gcs %s', e)
            return []

        return downloads_path

class TransformData:

    def __init__(self, local_db_path=None):
        self.local_db_path = local_db_path or 'tmp/data.db'
        os.makedirs(os.path.dirname(self.local_db_path), exist_ok=True)

    def _load_files_to_duckdb(
        self, table_name: str, files_to_load: list[str], file_format: str) -> bool:

        if file_format != '.json':
            logging.error('Only json files to duckdb are implemented')
            return False

        conn = duckdb.connect(self.local_db_path)
        if file_format == '.json':
            try:
                conn.execute(f"""
                    create or replace table {table_name} as
                    select * from read_json(?)
                """, [files_to_load])
                logging.info('%s was created', table_name)
                conn.close()
            except Exception as e:
                logging.error('Failed to load json files to duckdb %s', e)
                conn.close()
                return False
        return True

    def _prepare_and_export(self, table_name: str, export_query: str, path: str) -> str:
        conn = duckdb.connect(self.local_db_path)
        try:
            conn.execute(export_query, [path])
            logging.info('%s table prepared and exported', table_name)
            conn.close()
        except Exception as e:
            logging.error('Failed to prepare data: %s', e)
            conn.close()
            return
        return path

    def process(
        self,
        table_name: str,
        files_to_load: list[str],
        file_format: str,
        export_query: str,
        path: str) -> str:

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._load_files_to_duckdb(table_name, files_to_load, file_format)
            self._prepare_and_export(table_name, export_query, path)
            return path
        except Exception as e:
            logging.error('Could not process table %s: %s', table_name, e)
            return ''


if __name__ == '__main__':

    extract = ExtractDataGCS(project_id=PROJECT_ID, bucket_id=BUCKET_ID)
    transform = TransformData()

    #uploading example file to gcs
    upload_to_gcs(PROJECT_ID, BUCKET_ID, 'examples/data.json', GCS_PREFIX)

    for table_name, config in TABLES_CONFIG.items():

        file_format = config.get('file_format', '.json')
        export_query = config.get('export_query')
        export_path = config.get('export_path')

        if not export_query or not export_path:
            logging.error('Missing configurations for table %s', table_name)
            continue

        download_files = extract.download_files(
            gcs_prefix=GCS_PREFIX,
            file_format=file_format
        )

        if not download_files:
            logging.error('could not download any files for table %s', table_name)
            continue

        processed_file = transform.process(
            table_name,
            download_files,
            file_format,
            export_query,
            export_path
        )

        parquet_to_bq(PROJECT_ID, DATASET_ID, TABLE_ID, processed_file)











