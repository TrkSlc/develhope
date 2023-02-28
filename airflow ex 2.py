from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from datetime import datetime, timedelta

def python_first_function(): 
	x = datetime.datetime.now()
	print(x)
default_dag_args = { 'start_date': datetime(2022, 1, 1), 'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=5), 'project_id': 1 }

with DAG("First_DAG", schedule_interval = None, default_args = default_dag_args) as dag:
	task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)
