from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_task():
    print("Start Task")

def end_task():
    print("End Task")

with DAG(
    dag_id="sequence_dag",
    description="Простой DAG с последовательными задачами",
    schedule_interval=None, 
    start_date=datetime(2024, 12, 1),  
    catchup=False,  
) as dag:
    start = PythonOperator(
        task_id="start_task",  
        python_callable=start_task, 
    )

    end = PythonOperator(
        task_id="end_task",  
        python_callable=end_task, 
    )

    start >> end
