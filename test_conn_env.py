# Filename: test_conn_env.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2020, 12, 30),
  'email': ['vipin@flowgi.io'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'catchup': False
}

dag = DAG('test_conn', schedule_interval='0 0 1 * *' ,
  default_args=default_args)
create_command = 'echo   $AIRFLOW_CONN_POSTGRES_MASTER'
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
