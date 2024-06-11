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

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# tahun = list(2021, 2018)
# dataset_file_2018 = "Data-Penumpang-Bus-Transjakarta-2018.csv"
dataset_file = "Data-Penumpang-Bus-Transjakarta.csv"
dataset_url_2018 = f"https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_938ee225406078843b1f63976ac2ec97?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae"
dataset_url_2019 = f"https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_f89907c7699e161d3f8b744016733e20?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae"
dataset_url_2021 = f"https://satudata.jakarta.go.id/backend/restapi/v1/export-dataset/filedata_org47_7c45d2c3816a9b9e52c8e9038f1439fb?secret_key=sdibKbc83EIJG4dRy7Bgi520MWkE7rxwQZgsKJjyJNJXmLq1alaFLWwYtfExADae"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'busway')
RAW_DATASET = os.environ.get("RAW_DATASET", 'raw')

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
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
    dag_id="etl-jkt",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['etl-jkt'],
) as dag:

    download_dataset_task_2018 = BashOperator(
        task_id="download_dataset_task_2018",
        bash_command=f"curl {dataset_url_2018} >> {path_to_local_home}/2018-{dataset_file}"
    )
    # download_dataset_task_2019 = BashOperator(
    #     task_id="download_dataset_task_2019",
    #     bash_command=f"curl {dataset_url_2019} >> {path_to_local_home}/{dataset_file}"
    # )
    download_dataset_task_2021 = BashOperator(
        task_id="download_dataset_task_2021",
        bash_command=f"curl {dataset_url_2021} >> {path_to_local_home}/2021-{dataset_file}"
    )

    format_to_parquet_2018 = PythonOperator(
        task_id="format_to_parquet_2018",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/2018-{dataset_file}",
            # "src_file": f"{path_to_local_home}/2021-{dataset_file}",
        },
    )
    format_to_parquet_2021 = PythonOperator(
        task_id="format_to_parquet_2021",
        python_callable=format_to_parquet,
        op_kwargs={
            # "src_file": f"{path_to_local_home}/2018-{dataset_file}",
            "src_file": f"{path_to_local_home}/2021-{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_2018 = PythonOperator(
        task_id="local_to_gcs_2018",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/2018-{parquet_file}",
            "local_file": f"{path_to_local_home}/2018-{parquet_file}",
            # "object_name": f"raw/2021-{parquet_file}",
            # "local_file": f"{path_to_local_home}/2021-{parquet_file}",
        },
    )
    local_to_gcs_2021 = PythonOperator(
        task_id="local_to_gcs_2021",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/2021-{parquet_file}",
            "local_file": f"{path_to_local_home}/2021-{parquet_file}",
        },
    )


    delete_dataset_csv_task = BashOperator(
        task_id="delete_dataset_csv_task",
        bash_command=f"rm -rf {path_to_local_home}/*-{dataset_file}",
        # bash_command=f"rm -rf {path_to_local_home}/2021-{dataset_file}",
    )

    delete_dataset_parquet_task = BashOperator(
        task_id="delete_dataset_parquet_task",
        bash_command=f"rm -rf {path_to_local_home}/*-{parquet_file}",
        # bash_command=f"rm -rf {path_to_local_home}/2021-{parquet_file}",
    )


    # # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    # local_to_gcs_task = PythonOperator(
    #     task_id="local_to_gcs_task",
    #     python_callable=upload_to_gcs,
    #     op_kwargs={
    #         "bucket": BUCKET,
    #         "object_name": f"raw/{parquet_file}",
    #         "local_file": f"{path_to_local_home}/{parquet_file}",
    #     },
    # )

    # create_external_table = BigQueryCreateExternalTableOperator(
    #     task_id="create_external_table",
    #     destination_project_dataset_table=f"{BIGQUERY_DATASET}.data_busway",
    #     bucket=BUCKET,
    #     source_objects=[f"gs://{BUCKET}/raw/{parquet_file}"],
    #     schema_fields=[
    #         {"name": "periode_data", "type": "STRING", "mode": "REQUIRED"},
    #         {"name": "jenis", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "kode_trayek", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "trayek", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "jumlah_penumpang", "type": "STRING", "mode": "NULLABLE"},
    #     ],
    # )

    bigquery_external_table_2018 = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_2018",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": RAW_DATASET,
                "tableId": "tj_data_busway_2018",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/2018-{parquet_file}"],
                "schema_fields":[
                    {"name": "pelanggan", "type": "STRING  ", "mode": "REQUIRED"},
                    {"name": "jumlah", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "periode_data", "type": "INT64", "mode": "REQUIRED"},
                ],
            },        
        },
    )

    bigquery_external_table_2021 = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_2021",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": RAW_DATASET,
                "tableId": "tj_data_busway_2021",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/2021-{parquet_file}"],
                "schema_fields":[
                    {"name": "periode_data", "type": "INT64", "mode": "REQUIRED"},
                    {"name": "jenis", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "kode_trayek", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "trayek", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "jumlah_penumpang", "type": "INT64", "mode": "REQUIRED"},
                ],
            },        
        },
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": 
                    """
                    CREATE OR REPLACE TABLE `busway.data_busway`
                    (
                        `periode` INT64,
                        `rute` STRING,
                        `jumlah_penumpang` INT64,
                        PRIMARY KEY (periode, rute) NOT ENFORCED,
                    )
                    AS
                    SELECT *
                    FROM
                    (
                    select periode_data as periode, pelanggan as rute, SAFE_CAST(REGEXP_REPLACE(jumlah, r'[^\d]', '') AS INT64) as jumlah_penumpang
                    from `raw.tj_data_busway_2018`
                    UNION ALL
                    select periode_data as periode, trayek as rute, jumlah_penumpang
                    from `raw.tj_data_busway_2021`
                    );
                    """,
                "useLegacySql": False,
            }
        },
        dag = dag
    )


    download_dataset_task_2018 >> format_to_parquet_2018 >> local_to_gcs_2018 >> bigquery_external_table_2018 >> bq_create_partitioned_table_job
    download_dataset_task_2021 >> format_to_parquet_2021 >> local_to_gcs_2021 >> bigquery_external_table_2021 >> bq_create_partitioned_table_job

    [local_to_gcs_2018, local_to_gcs_2021] >> delete_dataset_csv_task >> delete_dataset_parquet_task
