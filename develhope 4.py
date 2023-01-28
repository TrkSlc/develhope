import time import json from airflow import DAG from airflow.operators.postgres_operator import PostgresOperator from datetime import timedelta
from airflow.utils.dates import days_ago


create_query = """
CREATE TABLE Employee(
	Name varchar(255),
	Age int);	
"""

insert_data_query = """ 
INSERT INTO Employee (Name, Age)
	VALUES ('Tarik', 22)
	VALUES ('Must', 25)
	VALUES ('Gul', 38);	
"""

calculating_averag_age = """ 
SELECT avg(Age) as "Average_Age"
FROM Employee;	
"""

default_args = { 'owner': 'airflow','retries': 1, 'retry_delay': timedelta(minutes=5), }

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))

	create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "pc_local")
	insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "pc_local")
	group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "pc_local")

	create_table >> insert_data >> group_data`