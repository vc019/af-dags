# Filename: hello_SLA_dag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from cw_pagerduty_pkg.cw_pagerduty import trigger_incident
import logging

logger = logging.getLogger(__name__)


def cw_sla_missed_take_action(*args, **kwargs):
    # Logs to scheduler logs. Not in the scheduler UI.
    # Location $AIRFLOW_HOME/logs/scheduler/
    logger.info("************************************* SLA missed! ***************************************")
    incident_title = 'airflow integration test title'
    incident_details = 'Integration with airflow looks good. Here is some nice info'
    incident_priority = 'P1'
    trigger_incident(incident_title, incident_details, incident_priority)
    logger.info("************************************* incident created ***************************************")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 14),
    'email': ['vipin.chadha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=5)
}

dag = DAG('hello_sla_dag',
          schedule_interval='* * * * *',
          default_args=default_args,
          catchup=False,
          sla_miss_callback=cw_sla_missed_take_action
          )
next_command = 'echo   Awake now!'

t1 = BashOperator(
    task_id='sleep_for_10s',  # Cause the task to miss the SLA
    bash_command='sleep 10',
    dag=dag
)

t2 = BashOperator(
    task_id='next_task',
    bash_command=next_command,
    dag=dag
)

t2.set_upstream(t1)