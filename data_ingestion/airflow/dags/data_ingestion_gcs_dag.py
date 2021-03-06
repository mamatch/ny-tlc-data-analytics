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
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")


def download_file():
    """
    Download the if it doesn't exist
    """
    if not os.path.exists("{}/{}".format(path_to_local_home, dataset_file)):
        os.system("curl -sS {} > {}/{}".format(dataset_url, path_to_local_home, dataset_file))
        logging.info("Downloading completed")
    else:
        logging.info("File already exists.")


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accpet source file in CSV format")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket_name, object_name, local_file):
    """
    
    """
    creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    logging.info("creds path: {}".format(creds))

    if not os.path.exists(creds):
        logging.info("File doesn't exist.")

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5Mo
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    print(
        "File {} uploaded to {}.".format(
            local_file, object_name
        )
    )


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

    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=download_file,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
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
            "bucket_name": BUCKET,
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
