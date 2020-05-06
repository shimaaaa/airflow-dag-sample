import random
import string
import sys
from pathlib import Path
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.utils.file import TemporaryDirectory
import datetime
import logging

sys.path.append(str((Path(__file__).parent / "src").resolve()))
from operators.bigquery_count import BigQueryCountOperator

dataset = ""
project_id = models.Variable.get('gcp_project')
bq_sql = f"""
SELECT LCLid, ARRAY_AGG(Frame_1) as values
FROM `{dataset}.consumptions`
GROUP BY LCLid
HAVING count(1) = 588
"""

dag_dir = str((Path(__file__).parent).resolve())
bq_destination_table = f"{dataset}.tmp_out_table"
bucket = "vertical-reason-269813"
output_dir = "tmp/tmp-array.avro"
output_file = "tmp/tmp-array.avro/out-*.avro"
output_uri = f"gs://{bucket}/{output_file}"
venv_python = "~/.venv/bin/python"

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2018, 12, 1),
    "email_on_failure": True,
    "email_on_retry": False
}

with models.DAG(
        "test_datascience_sample",
        schedule_interval="@once",
        default_args=default_dag_args) as dag:

    with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
        avro_dir = tmp_dir + "/avro"
        data_path = tmp_dir + "/data.dat"

        convert_to_array = BigQueryOperator(
            task_id="convert_to_array",
            sql=bq_sql,
            destination_dataset_table=bq_destination_table,
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            dag=dag
        )
        cleanup_gcs = BashOperator(
            task_id="cleanup_gcs",
            bash_command=f"gsutil -m rm {output_uri}"
        )
        export_questions_to_gcs = BigQueryToCloudStorageOperator(
            task_id="export_array_to_gcs",
            source_project_dataset_table=bq_destination_table,
            destination_cloud_storage_uris=[output_uri],
            export_format="Avro"
        )
        download_avro = BashOperator(
            task_id="download_avro",
            bash_command=f"gsutil -m cp {output_uri} avro_dir"
        )
        count = BigQueryCountOperator(
            task_id="get_count",
            dataset="consumption_test",
            table="tmp_out_table",
            dag=dag
        )
        cmd = (
            venv_python +
            " " + dag_dir + "/src/convert_to_memmap.py" +
            ' --rows {{ ti.xcom_pull(task_ids="get_count") }}' +
            " --target-path " + avro_dir +
            " --target-attr values" +
            " --output-path " + data_path
        )
        convert = BashOperator(
            task_id="convert_avro",
            bash_command=cmd
        )

        (
            convert_to_array >>
            cleanup_gcs >>
            export_questions_to_gcs >>
            download_avro >>
            count >>
            convert
        )
