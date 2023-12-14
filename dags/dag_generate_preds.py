import logging
import tempfile
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from utils.dataset_tools import save_dataset

default_args = {
    "owner": "brenoAV",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def download_data_from_sidc(ti: TaskInstance):
    data = requests.get(url="https://www.sidc.be/SILSO/INFO/sndtotcsv.php", timeout=5)
    with tempfile.NamedTemporaryFile(
        mode="wb", delete=False, suffix=".csv"
    ) as temp_file:
        temp_file.write(data.content)
        temp_file_path = temp_file.name

    ti.xcom_push(key="csv_file_path", value=temp_file_path)


def format_data(ti: TaskInstance):
    import pandas as pd

    csv_file_path = ti.xcom_pull(
        task_ids="download_data_from_sidc", key="csv_file_path"
    )
    df = pd.read_csv(
        csv_file_path,
        sep=";",
        encoding="utf-8",
        names=[
            "year",
            "month",
            "day",
            "decimal_date",
            "daily_sunspot_number",
            "standard_deviation",
            "number_of_observations",
            "definitive_provisional_indicator",
        ],
        dtype={
            "year": int,
            "month": int,
            "day": int,
            "decimal_date": float,
            "daily_sunspot_number": int,
            "standard_deviation": float,
            "number_of_observations": int,
            "definitive_provisional_indicator": int,
        },
    )
    df["date"] = df[["year", "month", "day"]].apply(
        lambda row: datetime(year=row["year"], month=row["month"], day=row["day"]),
        axis=1,
    )
    with tempfile.NamedTemporaryFile(
        "wb", suffix=".parquet", delete=False
    ) as temp_file:
        save_dataset(df, temp_file.name)
        logging.info("Dataset %s saved", temp_file.name)

    ti.xcom_push(key="parquet_file_path", value=temp_file.name)


with DAG(
    dag_id="dag_generate_preds",
    default_args=default_args,
    start_date=datetime(2023, 11, 1),
    schedule_interval="@monthly",
) as dag:
    download_task = PythonOperator(
        task_id="download_data_from_sidc",
        python_callable=download_data_from_sidc,
    )

    prepare_task = PythonOperator(task_id="format_data", python_callable=format_data)

    create_bucket_task = S3CreateBucketOperator(
        task_id="create_bucket_aws",
        bucket_name="sunspot-number",
        aws_conn_id="minio_s3_conn",
    )
    upload_dataset_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_dataset_to_s3",
        filename='{{ ti.xcom_pull(task_ids="format_data", key="parquet_file_path") }}',
        dest_key="sunspot_number.parquet",
        dest_bucket="sunspot-number",
        aws_conn_id="minio_s3_conn",
        replace=True,
    )

    forecast_sunspot_number = BashOperator(
        task_id="forecast_sunspot_number",
        bash_command=(
            "python /usr/local/airflow/include/scripts/forecast_sunspot_number.py"
            " -sd 2020-05-01 -ed 2050-05-01"
            " -dp '{{task_instance.xcom_pull(task_ids='format_data', key='parquet_file_path')}}'"
        ),
    )

    upload_forecast_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_forecast_to_s3",
        filename='{{ ti.xcom_pull(task_ids="forecast_sunspot_number") }}',
        dest_key="sunspot_number_forecast.parquet",
        dest_bucket="sunspot-number",
        aws_conn_id="minio_s3_conn",
        replace=True,
    )

    download_task >> prepare_task
    prepare_task >> create_bucket_task
    create_bucket_task >> upload_dataset_to_s3
    upload_dataset_to_s3 >> forecast_sunspot_number
    forecast_sunspot_number >> upload_forecast_to_s3
