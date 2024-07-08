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

dataset_file = "BMT-Data-Penumpang-BMT-Bandung.csv"
dataset_bmt = f"https://opendata.bandung.go.id/api/bigdata/dinas_perhubungan/jumlah_penumpang_trans_metro_bandung_tmb_berdasarkan_koridor?download=csv"
# dataset_feeder
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'busway')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')
TABLE_BUS = "busrapidtransit"

def format_to_pq(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in csv format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
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

# DAG Task for declaration Airflow Orchestration
with DAG(
    dag_id="etl-transmetrobandung",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-bdg'],
) as dag:

    download_data = BashOperator(
        task_id="download_data",
        bash_command=f"curl {dataset_bmt} >> {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_pq,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/bandung/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    delete_local_data = BashOperator(
        task_id="delete_local_data",
        bash_command=f"rm -rf {path_to_local_home}/*-Data-Penumpang-*",
    )

    # Create external temp table
    bigquery_external_table = BigQueryInsertJobOperator(
        task_id=f"bigquery_external_table",
        configuration={
            "query": {
                "query": 
                    f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.raw.brt_transmetrobandung`
                    OPTIONS (
                        format ="PARQUET",
                        uris = ['gs://{BUCKET}/raw/bandung/{parquet_file}']
                    );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )

    # Create a partitioned table from external table
    bq_create_table = BigQueryInsertJobOperator(
        task_id=f"bq_create_table",
        configuration={
            "query": {
                "query": 
                    f"""
                    CREATE TABLE IF NOT EXISTS `busway.{TABLE_BUS}`
                    (
                    `periode` DATE,
                    `rute` STRING,
                    `jumlah_penumpang` INT64,
                    `kota` STRING,
                    PRIMARY KEY (periode, rute) NOT ENFORCED,
                    );
                    
                    MERGE `busway.{TABLE_BUS}` AS t
                    USING (
                        SELECT
                        CAST(CAST(tahun || 
                            CASE bulan 
                            WHEN 'JANUARI' THEN '01'
                            WHEN 'FEBRUARI' THEN '02'
                            WHEN 'MARET' THEN '03'
                            WHEN 'APRIL' THEN '04'
                            WHEN 'MEI' THEN '05'
                            WHEN 'JUNI' THEN '06'
                            WHEN 'JULI' THEN '07'
                            WHEN 'AGUSTUS' THEN '08'
                            WHEN 'SEPTEMBER' THEN '09'
                            WHEN 'OKTOBER' THEN '10'
                            WHEN 'NOVEMBER' THEN '11'
                            WHEN 'DESEMBER' THEN '12'
                            END AS STRING) AS DATE FORMAT "yyyyMM") AS  periode,
                            koridor AS rute, SAFE_CAST(jumlah_penumpang AS INT64) AS jumlah_penumpang, 'Bandung' AS kota
                        FROM `raw.brt_transmetrobandung`
                    ) AS s
                    ON t.periode = s.periode and t.rute = s.rute
                    WHEN NOT MATCHED THEN INSERT (periode, rute, jumlah_penumpang, kota) 
                    VALUES (s.periode, s.rute, s.jumlah_penumpang, s.kota);
                    """,
                "useLegacySql": False,
            }
        },
    )


    # [download_dataset_task_2018 ,download_dataset_task_2021] >> format_to_parquet_task >> local_to_gcs >> [bigquery_external_table_2018, bigquery_external_table_2021] >> bq_create_partitioned_table_job
    download_data >> format_to_parquet >> local_to_gcs >> bigquery_external_table >> bq_create_table
    
    local_to_gcs >> delete_local_data
