from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLThresholdCheckOperator
)
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id='dq_check_dag', 
    schedule_interval='@once', 
    start_date=datetime(2022, 7, 1),
    catchup=False) as dag:

    start_dag = DummyOperator(task_id="start_dag")
    end_dag = DummyOperator(task_id="end_dag")

    with TaskGroup(group_id="row_quality_checks") as quality_check_group:

        for col in ["quantity", 
                    "unit_price", 
                    "cogs", 
                    "tax_5_perc", 
                    "total", 
                    "gross_income", 
                    "gross_income_perc"]:

            threshold_check = SQLCheckOperator(
                task_id=f"check_min_{col}",
                sql=f"SELECT MIN({col}) FROM supermarket_project.nds_mart;",
                conn_id="nds_db"
    )
    duplicate_invoice_id = SQLThresholdCheckOperator(
        task_id="duplicate_invoice_id",
        sql = "select count(1) from supermarket_project.nds_mart group by " \
              "invoice_id",
        conn_id="nds_db",
        min_threshold=1,
        max_threshold=1
    )
    start_dag >> [quality_check_group, duplicate_invoice_id] >> end_dag
