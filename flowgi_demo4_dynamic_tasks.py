from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3PrefixSensor
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

s3_bucketname = Variable.get("s3_bucketname", deserialize_json=False)
s3_loc = Variable.get("s3_loc", deserialize_json=False)
s3_prefix = Variable.get("s3_prefix_demo4", deserialize_json=False)

start_task = DummyOperator(task_id='demo4_start', dag=dag)

s3_prefix_sensor = S3PrefixSensor(
    task_id='s3_prefix_sensor',
    bucket_name=s3_bucketname,
    prefix=s3_prefix,
    poke_interval=5,
    timeout=10,
    aws_conn_id='customer1_s3_logs',
    dag=dag
)


def flowgi_process_file(s3_bucket, s3_key):
    print("Hello World")


def flowgi_dynamic_task_generator():
    print("Hello from call back! - Let's launch multiple tasks based on the files found")
    end_task = DummyOperator(task_id='end', dag=dag)
    v_s3hook = S3Hook(aws_conn_id='customer1_demo_s3')
    keys = v_s3hook.list_keys(s3_bucketname, s3_prefix)
    for key in keys:
        k = key.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"})
        print("Creating a task for key %s", key)
        process_task = PythonOperator(
            task_id='Process_file_' + k,
            python_callable=flowgi_process_file,
            op_kwargs={'s3_bucket': s3_bucketname, 's3_key': k},
            dag=dag
        )
        s3_prefix_sensor >> process_task >> end_task


generate_tasks = PythonOperator(
    task_id='Files_Arrived',
    python_callable=flowgi_dynamic_task_generator,
    trigger_rule='all_success',
    dag=dag,
)

start_task >> s3_prefix_sensor >> generate_tasks
