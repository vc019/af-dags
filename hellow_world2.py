# Filename: hello_world2.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2018, 5, 30),
  'email': ['airflow@example.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
# 'queue': 'bash_queue',
# 'pool': 'backfill',
# 'priority_weight': 10,
# 'end_date': datetime(2016, 1, 1),
}

dag = DAG('hello_world2', schedule_interval='0 0 * * *' ,
  default_args=default_args)
  create_command = 'echo   HELLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO '
t1 = BashOperator(
  task_id='print_date',
  bash_command='date',
  dag=dag
)

t2 = BashOperator(
  task_id= 'myBashTest',
  bash_command=create_command,
  dag=dag
)
t2.set_upstream(t1)
