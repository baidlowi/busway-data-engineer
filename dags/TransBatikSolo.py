import os
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.email import send_email

from google.cloud import storage
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas
import pyarrow as pa

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

year = 2023
month = "agustus"

# Define the website URL
url = f"https://data.surakarta.go.id/dataset/data-penumpang-bst-bulan-{month}-{year}"
response = requests.get(url)
if response.status_code == 200:
  soup = BeautifulSoup(response.content, 'html.parser')
  data_elements = soup.find_all(attrs={"data-id": True})
  if data_elements:
    for element in data_elements:
      data_id = element.get('data-id')

dataset_file = f"{year}-{month}-Data-Penumpang-Bus-Surakarta.xlsx"
dataset_url = f"https://data.surakarta.go.id/dataset/data-penumpang-bst-bulan-{month}-{year}/resource/{data_id}/download"
    
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.xlsx', '.parquet')
GCS_PARQUET = f"raw/surakarta/{parquet_file}"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'busway')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')

def format_to_parquet(src_file):
    if not src_file.endswith('.xlsx'):
        return
    table = pandas.read_excel(src_file, skiprows=1)

    # Define a function to extract year and month from filename
    def extract_year_month(src_file):
      pattern = r"(\d{4})-(\w+)" 

      match = re.search(pattern, src_file)
      if match:
        year, month = match.groups()
        return year, month
      else:
        return None, None

    year, month = extract_year_month(src_file)
    table['year'] = year
    table['month'] = month

    # Save the modified DataFrame to a new file (optional)
    table = pa.Table.from_pandas(table.astype(str))
    pq.write_table(table, src_file.replace('.xlsx', '.parquet'))

def check_file_exists(**kwargs):
    gcs_list_operator = GoogleCloudStorageObjectSensor(
        task_id="check_file_exists",
        bucket=BUCKET,
        object=GCS_PARQUET,
        poke_interval=10,
        timeout=60,
    )
    results = gcs_list_operator.poke(context=kwargs)

    if results:
        raise AirflowSkipException
    else:
        return True

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

# Task Flow 
with DAG(
    dag_id="etl-surakarta",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-srkt'],
) as dag:
    # Download Dataset
    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl {dataset_url} >> {path_to_local_home}/{dataset_file}"
    )

    # Format dataset csv to parquet
    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )
    
    # Check if target Parquet file exists on GCS
    check_file_task = PythonOperator(
        task_id="check_file_task",
        python_callable=check_file_exists,
        provide_context=True,
        on_failure_callback=lambda context: context["task_instance"].xcom_push(key="skipped", value=True),
    )

    # Upload file local to GCS
    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/surakarta/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    # Delete local file dataset
    delete_local_data = BashOperator(
        task_id="delete_local_data",
        bash_command=f"rm -rf {path_to_local_home}/*Data-Penumpang-*",
    )

    # Create external temp table
    bigquery_external_table = BigQueryInsertJobOperator(
        task_id=f"bigquery_external_table",
        configuration={
            "query": {
                "query": 
                    f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.raw.brt_transbatiksolo`
                    OPTIONS (
                        format ="PARQUET",
                        uris = ['gs://{BUCKET}/{GCS_PARQUET}']
                    );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )
    
    # Create table and transform data from external table
    bq_create_table = BigQueryInsertJobOperator(
        task_id=f"bq_create_table",
        configuration={
            "query": {
                "query": 
                    f"""
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
                        SELECT
                        CAST(CAST(year || 
                            CASE month 
                            WHEN 'januari' THEN '01'
                            WHEN 'februari' THEN '02'
                            WHEN 'maret' THEN '03'
                            WHEN 'april' THEN '04'
                            WHEN 'mei' THEN '05'
                            WHEN 'juni' THEN '06'
                            WHEN 'juli' THEN '07'
                            WHEN 'agustus' THEN '08'
                            WHEN 'september' THEN '09'
                            WHEN 'oktober' THEN '10'
                            WHEN 'november' THEN '11'
                            WHEN 'desember' THEN '12'
                            END AS STRING) AS DATE FORMAT "yyyyMM") AS  periode,
                            Rute AS rute, SAFE_CAST(Total_Penumpang AS INT64) AS jumlah_penumpang, 'Surakarta' AS kota
                        FROM `raw.srkt_busway`
                    ) AS s
                    ON t.periode = s.periode and t.rute = s.rute
                    WHEN NOT MATCHED THEN INSERT (periode, rute, jumlah_penumpang, kota) 
                    VALUES (s.periode, s.rute, s.jumlah_penumpang, s.kota);
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )

    # Flow in Airflow
    download_dataset >> format_to_parquet >> check_file_task  >> local_to_gcs >> bigquery_external_table >> bq_create_table
    local_to_gcs >> delete_local_data