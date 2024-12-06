from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция для выполнения
def print_hello():
    print("Hello from Airflow!")

# Определение DAG
with DAG(
    dag_id="simple_test_dag",  # Уникальный идентификатор DAG
    schedule_interval=None,  # Запускается только вручную
    start_date=datetime(2024, 12, 1),  # Дата начала
    catchup=False,  # Не запускать пропущенные задачи
) as dag:
    # Оператор Python
    hello_task = PythonOperator(
        task_id="say_hello",  # Идентификатор задачи
        python_callable=print_hello,  # Указание на функцию
    )
