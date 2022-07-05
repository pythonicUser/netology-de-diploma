from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG(
    dag_id='to_dwh_dag', 
    schedule_interval='@once', 
    start_date=datetime(2022, 7, 1),
    catchup=False) as dag:
    
    start_dag = DummyOperator(task_id='start_dag')

    load_data_to_dwh = BashOperator(task_id='load_data_to_dwh', 
                                    bash_command='python3 /opt/airflow/dags/etl_scripts/from_nds_to_dwh.py')
    
    
    end_dag = DummyOperator(task_id='end_dag')

    start_dag >> load_data_to_dwh >> end_dag