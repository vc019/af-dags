from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
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

dag = DAG('flowgi_demo5_email_notifications',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=False
          )

s3_bucketname = Variable.get("demo4_s3_bucketname", deserialize_json=False)
s3_chk_key = Variable.get("demo4_s3_key", deserialize_json=False)
s3_key_prefix = Variable.get("demo4_s3_key_prefix", deserialize_json=False)

v_email_id = Variable.get("demo5_email_notify", deserialize_json=False)
v_email_subject = Variable.get("demo5_email_subject", deserialize_json=False)
v_email_body = Variable.get("demo5_email_body", deserialize_json=False)

start_task = DummyOperator(task_id='demo5_start', dag=dag)

notify_via_email = EmailOperator(
    task_id="notify_via_email",
    to=v_email_id,
    subject=v_email_subject,
    html_content=v_email_body,
    trigger_rule='all_success',
    dag=dag)

s3_sensor = S3KeySensor(
    task_id='check_if_files_arrived',
    poke_interval=5,
    timeout=10,
    soft_fail=True,
    bucket_key=s3_chk_key,
    bucket_name=s3_bucketname,
    wildcard_match=True,
    aws_conn_id='customer1_demo_s3',
    dag=dag)


def flowgi_process_file(s3_bucket, s3_key):
    print("Hello World")
    print("Bucket Name:" + s3_bucket)
    print("Key Name:" + s3_key)


start_task >> s3_sensor

v_s3hook = S3Hook(aws_conn_id='customer1_demo_s3')
keys = v_s3hook.list_keys(s3_bucketname, s3_key_prefix)
s3_key_prefix = s3_key_prefix + '/'
print("S3 Key Prefix:" + s3_key_prefix)
for key in keys:
    k = ''.join(e for e in key if e.isalnum())
    print("Creating a task for key: " + key)
    if key != s3_key_prefix and key != s3_chk_key:
        process_task = PythonOperator(
            task_id='Process_file_' + k,
            python_callable=flowgi_process_file,
            op_kwargs={'s3_bucket': s3_bucketname, 's3_key': key},
            dag=dag
        )
        v_email_body = v_email_body + '\n' + 'Processed file from bucket:' + s3_bucketname + ', file path:'+key
        s3_sensor >> process_task >> notify_via_email
