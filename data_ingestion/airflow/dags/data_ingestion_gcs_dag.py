import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GSC_BUCKET")


dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/{}".format(dataset_file)
path_to_local_home = os.environ.get("AIRFLOW_HOME", '/opt/airflow/')
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accpet source file in CSV format")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    
    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5Mo
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owners": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    'data_ingestion_gcs_dag',
    default_args=default_args,
    description='A simple tutorial DAG to ingestion data to gsc',
    schedule_interval="@daily",
    tags=['ny_de'],
) as dag:

    download_data_task = BashOperator(
        task_id="download_data_task",
        bash_command="curl -sS {} > {}/{}".format(dataset_url, path_to_local_home, dataset_file),
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": "{}/{}".format(path_to_local_home, dataset_file)
        },
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/{}".format(parquet_file),
            "local_file": "{}/{}".format(path_to_local_home, parquet_file)
        },
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": ["gs://{}/raw/{}".format(BUCKET, parquet_file)]
            }
        }
    )

    download_data_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
