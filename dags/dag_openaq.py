from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import load_locations, load_countries, load_parameters, load_measures

default_args = {"retries": 3, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="update_openaq_dw_hourly",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    schedule_interval="0 * * * *",
    default_args=default_args,
) as dag:
    # task con dummy operator
    dummy_start_task = DummyOperator(task_id="start")

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift",
        sql="sql/creates.sql",
        hook_params={"options": "-c search_path=nicolasmvinciguerra_coderhouse"},
    )

    create_stored_procedures_task = PostgresOperator(
        task_id="create_stored_procedures",
        postgres_conn_id="redshift",
        sql="sql/stored_procedures.sql",
        hook_params={"options": "-c search_path=nicolasmvinciguerra_coderhouse"},
    )

    load_locations_data_task = PythonOperator(
        task_id="load_stations_data",
        python_callable=load_locations,
        op_kwargs={"config_file": "/opt/airflow/config/config.ini", "limit": "2500"},
    )

    load_countries_data_task = PythonOperator(
        task_id="load_countries_data",
        python_callable=load_countries,
        op_kwargs={"config_file": "/opt/airflow/config/config.ini", "limit": "500"},
    )

    load_parameters_data_task = PythonOperator(
        task_id="load_parameters_data",
        python_callable=load_parameters,
        op_kwargs={"config_file": "/opt/airflow/config/config.ini", "limit": "100"},
    )

    load_measures_data_task = PythonOperator(
        task_id="load_measures_data",
        python_callable=load_measures,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "start_date": "{{ data_interval_start }}",
            "end_date": "{{ data_interval_end }}",
        },
    )

    dummy_end_task = DummyOperator(task_id="end")

    dummy_start_task >> create_tables_task
    dummy_start_task >> create_stored_procedures_task
    create_stored_procedures_task >> load_locations_data_task
    load_locations_data_task >> load_countries_data_task
    create_stored_procedures_task >> load_parameters_data_task
    create_stored_procedures_task >> load_measures_data_task
    load_countries_data_task >> dummy_end_task
    load_parameters_data_task >> dummy_end_task
    load_measures_data_task >> dummy_end_task
