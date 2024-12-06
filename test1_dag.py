from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="simple_bash_sequence_dag",
    description="Простой DAG с последовательными bash задачами",
    schedule_interval=None, 
    start_date=datetime(2024, 12, 1),  
    catchup=False,  
) as dag:

    task1 = BashOperator(
        task_id="task_1",  
        bash_command="echo 'Task 1 is starting...'",
    )

    task2 = BashOperator(
        task_id="task_2",  
        bash_command="echo 'Task 2 is starting...'",
    )

    task1 >> task2  
