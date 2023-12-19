from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.main import alert_avg_values

default_args = {"retries": 3, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="email_alerting_avg_daily",
    description="Send alerting emails with countries whose setting parameters are above the average.",
    start_date=datetime(2023, 12, 17),
    catchup=False,
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    dummy_start_task = DummyOperator(task_id="start")

    alerting_email_task = PythonOperator(
        task_id="alerting_email",
        python_callable=alert_avg_values,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "exec_date": "{{ds}}",
            "sender_email": "{{var.value.gmail_account}}",
            "private_key": "{{var.value.gmail_secret}}",
        },
    )

    dummy_end_task = DummyOperator(task_id="end")

    dummy_start_task >> alerting_email_task >> dummy_end_task
