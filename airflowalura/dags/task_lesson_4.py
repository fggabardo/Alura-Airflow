from airflow.models import DAG
from airflow.utils.dates import days_ago
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_lesson_4',
    start_date = pendulum.today('UTC').add(days=-1),
    schedule_interval = '@daily'
) as dag:
    
    def cumprimentos():
        print("Welcome to Airflow!")
    
    task_1 = PythonOperator(
        task_id = 'cumprimentos',
        python_callable=cumprimentos
        )
