from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='to_nds_dag', 
    schedule_interval='@once', 
    start_date=datetime(2022, 7, 1),
    catchup=False) as dag:
    
    start_dag = DummyOperator(task_id='start_dag')

    load_data_to_nds = BashOperator(task_id='load_data_to_nds', 
                                    bash_command='python3 /opt/airflow/dags/etl_scripts/from_url_to_nds.py')
    
    refresh_view = PostgresOperator(
        task_id="refresh_view",
        postgres_conn_id="nds_db",
        sql="REFRESH MATERIALIZED VIEW supermarket_project.nds_mart;"
    )
    
    end_dag = DummyOperator(task_id='end_dag')

    start_dag >> load_data_to_nds >> refresh_view >> end_dag