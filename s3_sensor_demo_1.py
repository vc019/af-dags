from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 27),
    'email': ['vipin.chadha@gmail.com'],
    'email_on_failure': False,
    'max_active_runs': 1,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_sensor_dag_1',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False
          )

s3_sensor = S3KeySensor(
    task_id='s3_check_if_file_present',
    poke_interval=2,
    timeout=10,
    soft_fail=True,
    bucket_key='s3://cloudwalker-spark3/flowgi/sensor_demo/my_testfile.txt',
    bucket_name=None,
    dag=dag)

failure_task = BashOperator(
    task_id='in_case_of_failure',
    depends_on_past=False,
    bash_command='echo file not found in s3 bucket',
    trigger_rule='all_failed',
    dag=dag)

def my_custom_func():
    print("Hello from call back!")

success_task = PythonOperator(
    task_id='in_case_of_success',
    python_callable=my_custom_func,
    trigger_rule='all_done',
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

s3_sensor >> failure_task >> end_task
s3_sensor >> success_task >> end_task