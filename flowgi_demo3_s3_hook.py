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

dag = DAG('flowgi_demo3_s3_hook',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False
          )
s3_bucketname = Variable.get("demo3_src_s3_bucketname", deserialize_json=False)
s3_loc = Variable.get("demo3_src_s3_loc", deserialize_json=False)

s3_sensor = S3KeySensor(
    task_id='s3_check_if_file_present',
    poke_interval=5,
    timeout=10,
    soft_fail=True,
    bucket_key=s3_loc,
    bucket_name=s3_bucketname,
    aws_conn_id='customer1_demo_s3',
    dag=dag)


def my_custom_func():
    print("Hello from call back! - Let me move the files")
    v_s3hook = S3Hook(aws_conn_id='customer1_demo_s3')
    s3_dest_bucket = Variable.get("demo3_dest_s3_bucket", deserialize_json=False)
    s3_dest_key = Variable.get("demo3_dest_dest_loc", deserialize_json=False)
    v_s3hook.copy_object(source_bucket_name=s3_bucketname, source_bucket_key=s3_loc, dest_bucket_name=s3_dest_bucket,
                         dest_bucket_key=s3_dest_key)
    v_s3hook.delete_objects(s3_bucketname, [s3_loc])


process_task = PythonOperator(
    task_id='Files_Arrived',
    python_callable=my_custom_func,
    trigger_rule='all_success',
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

s3_sensor >> process_task >> end_task
