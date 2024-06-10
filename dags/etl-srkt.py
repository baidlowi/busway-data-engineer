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
import pyarrow as pa
import pandas

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "Data-Penumpang-Bus-Surakarta.xlsx"
dataset_url = f"https://data.surakarta.go.id/dataset/67bd959a-d8aa-4664-88e9-3bdf7502e511/resource/6baec2ca-9ec9-433c-b9ab-6b082a480622/download/report-rute-bengawan-solo-trans-surakarta-1-january-2022-to-31-january-2022.xlsx"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.xlsx', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'busway')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')

def format_to_parquet(src_file):
    if not src_file.endswith('.xlsx'):
        logging.error("Can only accept source files in xlsx format, for the moment")
        return
    table = pandas.read_excel(src_file, skiprows=1)
    table = pa.Table.from_pandas(table.astype(str))
    pq.write_table(table, src_file.replace('.xlsx', '.parquet'))


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
    dag_id="etl-surakarta",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-srkt'],
) as dag:

    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl {dataset_url} >> {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
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
        "object_name": f"raw/{parquet_file}",
        "local_file": f"{path_to_local_home}/{parquet_file}",
    },
)

    delete_local_data = BashOperator(
        task_id="delete_local_data",
        bash_command=f"rm -rf {path_to_local_home}/*Data-Penumpang-*",
    )

    bigquery_external_table = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": RAW_DATASET,
                "tableId": "srkt_data_busway",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                "schema_fields":[
                    {"name": "pelanggan", "type": "STRING  ", "mode": "NULLABLE"},
                    {"name": "jumlah", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "periode_data", "type": "INT64", "mode": "REQUIRED"},
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
                    CREATE OR REPLACE TABLE `busway.data_busway_srkt`
                    (
                        `Status` STRING,
                        `trayek` STRING,
                        `jumlah_penumpang` INT64,
                        PRIMARY KEY (jumlah_penumpang) NOT ENFORCED,
                    )
                    AS
                    SELECT *
                    FROM
                    (
                        select Status, Rute as trayek, safe_cast(Total_Penumpang as INT64) as jumlah_penumpang
                        from `raw.srkt_data_busway`
                    );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )


    download_dataset >> format_to_parquet >> local_to_gcs >> bigquery_external_table >> bq_create_table
    
    local_to_gcs >> delete_local_data

    CREATE OR REPLACE TABLE `busway.data_busway`
(
    `periode_data` INT64,
    `trayek` STRING,
    `jumlah_penumpang` STRING,
    PRIMARY KEY (periode_data, trayek) NOT ENFORCED,
)
AS
SELECT *
FROM
(
    select safe_cast(periode_data AS INT64) as periode_data, pelanggan as trayek, jumlah as jumlah_penumpang
    from `raw.tj_data_busway_2018`
    UNION ALL
    select safe_cast(periode_data AS INT64) as periode_data, trayek, safe_cast(jumlah_penumpang as STRING) as jumlah_penumpang
    from `raw.tj_data_busway_2021`
);