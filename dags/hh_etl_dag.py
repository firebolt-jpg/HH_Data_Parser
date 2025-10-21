from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к папке scripts, чтобы импортировать наши модули
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from hh_parser import parse_hh_vacancies_api

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'hh_vacancies_etl',
    default_args=default_args,
    description='Парсинг вакансий',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hh', 'etl', 'vacancies', 'data_analyst', 'data_engineer', 'api'],
    max_active_runs=1,
) as dag:

    # Задача для парсинга и загрузки данных через API
    parse_and_load_task = PythonOperator(
        task_id='parse_and_load_hh_vacancies',
        python_callable=parse_hh_vacancies_api,
    )

    parse_and_load_task