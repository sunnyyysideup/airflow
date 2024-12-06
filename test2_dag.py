"""
This DAG demonstrates a simple Airflow workflow that prints a message.
"""

from datetime import datetime  
from airflow import DAG 
from airflow.operators.python import PythonOperator

def print_hello():
    """
    This function prints a greeting message.
    """
    print("Hello from Airflow!")

with DAG(
    dag_id="simple_test_dag", 
    schedule_interval=None,  
    start_date=datetime(2024, 12, 1),
    catchup=False,  
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",  
        python_callable=print_hello,  
    )
