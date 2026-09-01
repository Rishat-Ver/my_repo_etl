import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

import psycopg
import pydantic
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'concurrency': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

API_host = 'https://de-start-sprint-etl-airflow-api.de.education-services.ru'


class UserOrderModel(pydantic.BaseModel):
    id: int
    uniq_id: str
    date_time: datetime
    city_id: int
    city_name: str
    customer_id: int
    first_name: str
    last_name: str
    item_id: int
    item_name: str
    quantity: int
    payment_amount: int

class UserActivityModel(pydantic.BaseModel):
    id: int
    uniq_id: str
    date_time: datetime
    action_id: int
    customer_id: int
    quantity: int


def parse_user_order_log():
    payload = {'limit': '20000000', 'filter': {'date': '2024-09-19'}}
    resp = requests.post(urljoin(API_host, 'user_order_log'), data=json.dumps(payload))
    resp.raise_for_status()
    msg = resp.json()
    for row in msg:
        respmodel = UserOrderModel(**row)
        print(*respmodel.model_dump().values())

def parse_user_activity_log():
    payload = {'limit': '20000000', 'filter': {'date': '2024-09-19'}}
    resp = requests.post(urljoin(API_host, 'user_activity_log'), data=json.dumps(payload))
    resp.raise_for_status()
    msg = resp.json()
    for row in msg:
        respmodel = UserActivityModel(**row)
        print(*respmodel.model_dump().values())


with DAG('api_data_load',
         default_args=default_args,
         start_date=datetime(2024, 1, 1),
         catchup=False,
         schedule_interval='@once',
         max_active_runs=1
) as dag:
    user_order_task = PythonOperator(
        task_id='user_order_task',
        python_callable=parse_user_order_log,
        provide_context=True
    )
    user_activity_task = PythonOperator(
        task_id='user_activity_task',
        python_callable=parse_user_activity_log,
        provide_context=True
    )

    user_order_task >> user_activity_task