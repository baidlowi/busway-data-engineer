import os
import logging
import subprocess

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from google.cloud import storage
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
import glob

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

data_sources = [
    {'url': 'https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_938ee225406078843b1f63976ac2ec97?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae',
        'dataset_file': '2018-Data-Penumpang-Bus-Transjakarta.csv'},
    {'url': 'https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_f89907c7699e161d3f8b744016733e20?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae', 
        'dataset_file': '2019-Data-Penumpang-Bus-Transjakarta.csv'},
    {'url': 'https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_7c45d2c3816a9b9e52c8e9038f1439fb?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae', 
        'dataset_file': '2021-Data-Penumpang-Bus-Transjakarta.csv'},
]

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'busway')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')

def format_to_parquet(data_dir, output_dir, file_pattern="*.csv"):
  csv_files = glob.glob(f"{data_dir}/{file_pattern}")
  for csv_file in csv_files:
    output_filename = os.path.splitext(os.path.basename(csv_file))[0]
    df = pd.read_csv(csv_file)
    df.to_parquet(os.path.join(output_dir, f"{output_filename}.parquet"))

def check_file_exists(**kwargs):
    gcs_list_operator = GoogleCloudStorageObjectSensor(
        task_id="check_file_exists",
        bucket=BUCKET,
        object=f"raw/transjakarta/2021-Data-Penumpang-Bus-Transjakarta.parquet",
        poke_interval=10,
        timeout=60,
    )
    results = gcs_list_operator.poke(context=kwargs)

    if results:
        raise AirflowSkipException
    else:
        return True

def upload_to_gcs(bucket, source_dir, destination_dir=""):
  client = storage.Client()
  bucket = client.bucket(bucket)

  for filename in os.listdir(source_dir):
    if not filename.endswith(".parquet"):
      continue 
    source_path = os.path.join(source_dir, filename)
    destination_path = os.path.join(destination_dir, filename) if destination_dir else filename
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(source_path)

default_args = {
    "owner": "HASYIM",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="etl-transjakarta",
    schedule_interval="*/20 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-jkt'],
) as dag:

    # # Delete Local Dataset .csv and .parquet
    # delete_prev_dataset = BashOperator(
    #     task_id="delete_prev_dataset",
    #     bash_command=f"rm -rf {path_to_local_home}/*-Data-*",
    # )

    # Task download dataset from resource url
    download_tasks = [
        BashOperator(
            task_id=f'download_{source["dataset_file"]}',
            bash_command=f'curl {source["url"]} >> {path_to_local_home}/{source["dataset_file"]}',
        )
        for source in data_sources
    ]

    # Format dataset .csv to .parquet
    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "data_dir": f"{path_to_local_home}",
            "output_dir": f"{path_to_local_home}",
        },
    )

    # Check if target Parquet file exists on GCS
    # check_file_gcs = PythonOperator(
    #     task_id="check_file_gcs",
    #     python_callable=check_file_exists,
    #     provide_context=True,
    #     on_failure_callback=lambda context: context["task_instance"].xcom_push(key="skipped", value=True),
    # )

    # Upload dataset parquet to Google Storage
    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "destination_dir": f"raw/transjakarta",
            "source_dir": f"{path_to_local_home}",
        },

    # Delete Local Dataset .csv and .parquet
    delete_local_data = BashOperator(
        task_id="delete_local_data",
        bash_command=f"rm -rf {path_to_local_home}/*-Data-*",
    )

    # Create external temp table
    bigquery_external_table = BigQueryInsertJobOperator(
        task_id=f"bigquery_external_table",
        configuration={
            "query": {
                "query": 
                    f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.raw.brt_transjakarta_2018`
                    OPTIONS (
                        format ="PARQUET",
                        uris = ['gs://{BUCKET}/raw/transjakarta/2018-Data-Penumpang-Bus-Transjakarta.parquet']
                    );

                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.raw.brt_transjakarta_2019`
                    OPTIONS (
                        format ="PARQUET",
                        uris = ['gs://{BUCKET}/raw/transjakarta/2019-Data-Penumpang-Bus-Transjakarta.parquet']
                    );

                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.raw.brt_transjakarta_2021`
                    OPTIONS (
                        format ="PARQUET",
                        uris = ['gs://{BUCKET}/raw/transjakarta/2021-Data-Penumpang-Bus-Transjakarta.parquet']
                    );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": 
                    """
                    CREATE TABLE IF NOT EXISTS `busway.busrapidtransit`
                    (
                    `periode` DATE,
                    `rute` STRING,
                    `jumlah_penumpang` INT64,
                    `kota` STRING,
                    PRIMARY KEY (periode, rute) NOT ENFORCED,
                    );

                    MERGE `busway.busrapidtransit` AS t
                    USING (
                        SELECT periode, 
                        CASE
                        WHEN rute like 'KORIDOR 1' THEN 'Blok M - Kota'
                        WHEN rute like 'KORIDOR 1 (BLOK M - KOTA)' THEN 'Blok M - Kota'
                        WHEN rute like 'KORIDOR 2%' THEN 'Pulo Gadung 1 - Harmoni'
                        WHEN rute like 'KORIDOR 3%' THEN 'Kalideres - Pasar Baru'
                        WHEN rute like 'KORIDOR 4%' THEN 'Pulo Gadung 2 - Tosari'
                        WHEN rute like 'KORIDOR 5%' THEN 'Kampung Melayu - Ancol'
                        WHEN rute like 'KORIDOR 6%' THEN 'Ragunan - Dukuh Atas 2'
                        WHEN rute like 'KORIDOR 7%' THEN 'Kampung Rambutan - Kampung Melayu'
                        WHEN rute like 'KORIDOR 8%' THEN 'Lebak Bulus - Harmoni'
                        WHEN rute like 'KORIDOR 9%' THEN 'Pinang Ranti - Pluit'
                        WHEN rute like 'KORIDOR 10%' THEN 'Tanjung Priok - PGC 2'
                        WHEN rute like 'KORIDOR 11%' THEN 'Pulo Gebang - Kampung Melayu'
                        WHEN rute like 'KORIDOR 12%' THEN 'Penjaringan - Sunter Bouleverd Barat'
                        WHEN rute like 'KORIDOR 13%' THEN 'Ciledug - Tendean'
                        ELSE rute
                        END AS rute, jumlah_penumpang, kota
                        FROM (
                        SELECT
                            CAST(CAST(periode_data as STRING) AS  DATE FORMAT "yyyyMM") AS periode,
                            pelanggan as rute,
                            SAFE_CAST(REGEXP_REPLACE(jumlah, r'[^\d]', '') AS INT64) as jumlah_penumpang, 
                            'Jakarta' as kota
                        FROM `raw.brt_transjakarta_2018`
                        WHERE pelanggan like 'KORIDOR%' and periode_data IS NOT NULL

                        UNION ALL

                        SELECT
                            CAST(CAST(periode_data as STRING) AS  DATE FORMAT "yyyyMM") AS periode,
                            trayek as rute,
                            SAFE_CAST(REGEXP_REPLACE(jumlah_penumpang, r'[^\d]', '') AS INT64) as jumlah_penumpang, 
                            'Jakarta' as kota
                        FROM `raw.brt_transjakarta_2019`
                        WHERE jenis = 'BRT'

                        UNION ALL

                        SELECT
                            CAST(CAST(periode_data as STRING) AS  DATE FORMAT "yyyyMM") AS periode,
                            trayek as rute,
                            CAST(jumlah_penumpang AS INT64) as jumlah_penumpang, 
                            'Jakarta' as kota
                        FROM `raw.brt_transjakarta_2021`
                        WHERE jenis = 'BRT'
                        )
                    ) as s
                    ON t.periode = s.periode and t.rute = s.rute
                    WHEN NOT MATCHED THEN INSERT (periode, rute, jumlah_penumpang, kota) 
                    VALUES (s.periode, s.rute, s.jumlah_penumpang, s.kota);
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )

    #Airflow workflow
    download_tasks >> format_to_parquet >> local_to_gcs >> bigquery_external_table >> bq_create_partitioned_table_job

    local_to_gcs >> delete_local_data