from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="simple_test_dag",
    description="Простой DAG для проверки работы",
    schedule_interval=None,  
    start_date=datetime(2024, 12, 6),  
    catchup=False, и
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",  
        python_callable=print_hello,  
    )
