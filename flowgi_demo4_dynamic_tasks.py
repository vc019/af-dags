from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 27),
    'email': ['vipin@flowgi.io'],
    'email_on_failure': False,
    'max_active_runs': 1,
    'email_on_retry': False,
    'retries': 50,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('flowgi_demo4_dynamic_tasks',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False
          )

s3_bucketname = Variable.get("demo4_s3_bucketname", deserialize_json=False)
s3_key = Variable.get("demo4_s3_key", deserialize_json=False)
s3_key_prefix = Variable.get("demo4_s3_key_prefix", deserialize_json=False)

start_task = DummyOperator(task_id='demo4_start', dag=dag)
end_task = DummyOperator(task_id='demo4_end', dag=dag)

s3_sensor = S3KeySensor(
    task_id='check_if_files_arrived',
    poke_interval=5,
    timeout=10,
    soft_fail=True,
    bucket_key=s3_key,
    bucket_name=s3_bucketname,
    wildcard_match=True,
    aws_conn_id='customer1_demo_s3',
    dag=dag)


def flowgi_process_file(s3_bucket, s3_key):
    print("Hello World")


v_s3hook = S3Hook(aws_conn_id='customer1_demo_s3')
keys = v_s3hook.list_keys(s3_bucketname, s3_key_prefix)

for key in keys:
    k = key.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"})
    if key != s3_key_prefix:
        print("Creating a task for key: " + key)
        process_task = PythonOperator(
            task_id='Process_file_' + k,
            python_callable=flowgi_process_file,
            op_kwargs={'s3_bucket': s3_bucketname, 's3_key': k},
            dag=dag
        )
    s3_sensor >> process_task >> end_task

