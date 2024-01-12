import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)

def extract(date_from, date_to):
    response = requests.get(
        f'https://analytics.gaz.cenix-pro.com/integration-api/v1.0/export-results/online/prices?api_token=abf1357f4ec09be61cd600b00622c7c9f7eab7df5c100db020369c9ec4844bab&dateFrom={date_from}&dateTo={date_to}&extraFields=cityId,parsingCollectionId',
        verify=False,
    )
    return pd.json_normalize(response.json())

def transform(data, year):
    data['year'] = year
    return data

def load(data, year):
    dwh_engine.execute(
        f"""
        DELETE FROM sttgaz.stage_cenix
        WHERE year = {year}
        """
    )
    data.to_sql(
        f'stage_cenix',
        dwh_engine,
        schema='sttgaz',
        if_exists='append',
        index=False,        
    )
    
def etl(**context):
    year = context['execution_date'].year
    date_from = dt.date(year, 1, 1)
    date_to = dt.date(year + 1, 1, 1) - dt.timedelta(day=1)
    load(transform(extract(date_from, date_to), year), year)


#-------------- DAG -----------------

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'cenix_dag',
        default_args=default_args,
        description='Получение результата парсинга цен ЗЧ.',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        load_data = PythonOperator(
            task_id=f'Получение_календаря',
            python_callable=etl,
        )

        load_data

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> end
