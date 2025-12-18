import logging
import os
from google.cloud import storage
from google.cloud import bigquery
from pathlib import PurePosixPath

def upload_to_gcs(
    project_id: str,
    bucket_id: str,
    local_file_path: str,
    gcs_destination_path: str
    ) -> bool:

    try:
        file_name = os.path.basename(local_file_path)

        client = storage.Client(project_id)
        bucket = client.bucket(bucket_id)
        gcs_path = str(PurePosixPath(gcs_destination_path) / file_name)
        blob = bucket.blob(gcs_path)

        blob.upload_from_filename(local_file_path)
        return True

    except Exception as e:
        logging.error(f'Failed to upload file to GCS: %s', e)
        return False

def parquet_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    local_file_path: str
    ) -> bool:

    try:
        client = bigquery.Client(project=project_id)
        dataset_ref = f'{project_id}.{dataset_id}'
        table_ref = f'{dataset_ref}.{table_id}'

        try:
            client.get_dataset(dataset_ref)
        except Exception as e:
            logging.info('Could not get dataset %s. Creating ...', e)
            dataset = bigquery.Dataset(dataset_ref)
            client.create_dataset(dataset)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        with open(local_file_path, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            ) 
            job.result()

        logging.info('Load completed for table %s.', table_ref)
        return True

    except Exception as e:
        logging.error('Failed to load parquet to BigQuery: %s', e)
        return False
