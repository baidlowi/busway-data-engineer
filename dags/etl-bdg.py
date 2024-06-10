import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
# import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "Data-Penumpang-BMT-Bandung.csv"
dataset_bmt = f"https://opendata.bandung.go.id/api/bigdata/dinas_perhubungan/jumlah_penumpang_trans_metro_bandung_tmb_berdasarkan_koridor?download=csv"
# dataset_feeder
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'bmt')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')

def format_to_pq(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in csv format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "HASYIM",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="etl-bandung",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-bdg'],
) as dag:

    download_data_bmt = BashOperator(
        task_id="download_data_bmt",
        bash_command=f"curl {dataset_bmt} >> {path_to_local_home}/Bmt-{dataset_file}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_pq,
        op_kwargs={
            "src_file": f"{path_to_local_home}/Bmt-{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/bmt-{parquet_file}",
            "local_file": f"{path_to_local_home}/bmt-{parquet_file}",
        },
    )

    delete_local_data = BashOperator(
        task_id="delete_local_data",
        bash_command=f"rm -rf {path_to_local_home}/*-Data-Penumpang-*",
    )

    bigquery_external_table_bmt = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_bmt",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": RAW_DATASET,
                "tableId": "bdg_data_bmt",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/bmt-{parquet_file}"],
                "schema_fields":[
                    {"name": "koridor", "type": "STRING  ", "mode": "NULLABLE"},
                    {"name": "jumlah", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "tahun", "type": "INT64", "mode": "REQUIRED"},
                    {"name": "bulan", "type": "STRING", "mode": "REQUIRED"},
                ],
            },        
        },
    )

    # Create a partitioned table from external table
    bq_create_table = BigQueryInsertJobOperator(
        task_id=f"bq_create_table",
        configuration={
            "query": {
                "query": 
                    """
                    CREATE OR REPLACE TABLE `busway.data_bmt`
                        (
                            `tahun` INT64,
                            `bulan` STRING,
                            `trayek` STRING,
                            `jumlah_penumpang` INT64,
                            PRIMARY KEY (jumlah_penumpang) NOT ENFORCED,
                        )
                        AS
                        SELECT *
                        FROM
                        (
                            select koridor as trayek, jumlah as jumlah_penumpang, tahun, bulan
                            from `raw.bdg_data_bmt`
                        );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )


    # [download_dataset_task_2018 ,download_dataset_task_2021] >> format_to_parquet_task >> local_to_gcs >> [bigquery_external_table_2018, bigquery_external_table_2021] >> bq_create_partitioned_table_job
    download_data_bmt >> format_to_parquet >> local_to_gcs >> bigquery_external_table_bmt >> bq_create_table
    
    local_to_gcs >> elete_local_data
