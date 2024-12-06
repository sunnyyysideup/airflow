from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_1():
    print("Задача 1 выполнена!")

def task_2():
    print("Задача 2 выполнена!")

def task_3():
    print("Задача 3 выполнена!")

with DAG(
    dag_id="simple_task_dag",
    description="Простой DAG с несколькими задачами",
    schedule_interval=None, 
    start_date=datetime(2024, 12, 6),
    catchup=False,
) as dag:

    task_one = PythonOperator(
        task_id="task_1",
        python_callable=task_1,
    )

    task_two = PythonOperator(
        task_id="task_2",
        python_callable=task_2,
    )

    task_three = PythonOperator(
        task_id="task_3",
        python_callable=task_3,
    )

    task_one >> task_two >> task_three
