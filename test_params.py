import random
import string
import sys
from pathlib import Path
from airflow import DAG
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import datetime
import logging


default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2018, 12, 1),
    "email_on_failure": True,
    "email_on_retry": False
}

def branch_func(**context):
    area_id = context["dag_run"].conf.get("area_id")
    if area_id is None:
        return "get_all"
    return "get_specific"


with models.DAG(
        "test_params",
        schedule_interval="@once",
        default_args=default_dag_args) as dag:

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_func,
        dag=dag
    )
    get_all = BashOperator(
        task_id="get_all",
        bash_command="echo collect all",
        dag=dag
    )
    get_specific = BashOperator(
        task_id="get_specific",
        bash_command="echo collect {{ dag_run.conf['area_id'] }}",
        dag=dag
    )
    continue_op = DummyOperator(task_id='continue_task', trigger_rule='none_failed', dag=dag)
    stop_op = DummyOperator(task_id='stop_task', trigger_rule='none_failed', dag=dag)

    branch_op >> [get_all, get_specific] >> continue_op >> stop_op
