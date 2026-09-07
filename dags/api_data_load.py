##############################################################################
#  Импорты и константы
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

##############################################################################
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
    
def insert_user_order_log(**kwargs):
    ds = kwargs['ds']
    payload = {'limit': '20000000', 'filter': {'date': ds}}
    resp = requests.post(urljoin(API_host, 'user_order_log'), data=json.dumps(payload))
    resp.raise_for_status()
    data = resp.json()
    query = f"""INSERT INTO public.user_order_log( uniq_id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount)
    VALUES ( %(uniq_id)s, %(date_time)s, %(city_id)s, %(city_name)s, %(customer_id)s, %(first_name)s, %(last_name)s, %(item_id)s, %(item_name)s, %(quantity)s, %(payment_amount)s);"""
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            for row in data:
            respmodel = UserOrderModel(**row)
            cur.execute(query, respmodel.model_dump())
        print('committed')


def insert_user_activity_log(**kwargs):
    ds = kwargs['ds']
    payload = {'limit': '20000000', 'filter': {'date': ds}}
    resp = requests.post(urljoin(API_host, 'user_activity_log'), data=json.dumps(payload))
    resp.raise_for_status()
    data = resp.json()
    query = f"""INSERT INTO public.user_activity_log( uniq_id, date_time, action_id, customer_id, quantity)
    VALUES ( %(uniq_id)s, %(date_time)s, %(action_id)s, %(customer_id)s, %(quantity)s);"""
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            for row in data:
            respmodel = UserActivityModel(**row)
            cur.execute(query, respmodel.model_dump())
        print('committed')

##############################################################################
#  сборка таблиц измерений
def load_d_customer():
    query = """
    WITH
         api_data AS ( --актуальные на дату расчёта данные в источнике
             SELECT DISTINCT
             ON (uol.customer_id)
                 uol.customer_id,
                 uol.first_name,
                 uol.last_name
             FROM public.user_order_log uol
             WHERE
                 uol.date_time::date = '{{ds}}'
             ORDER BY uol.customer_id, date_time DESC
    )
    INSERT INTO public.d_customer_stg (id, customer_id, first_name, last_name)
    SELECT --получаем дельту и вставляем данные
           dim.id,
           uol.customer_id,
           uol.first_name,
           uol.last_name
    FROM api_data uol
             LEFT JOIN public.d_customer dim ON dim.customer_id = uol.customer_id
        AND '{{ds}}' BETWEEN start_date AND end_date
    WHERE ((uol.first_name != dim.first_name
        OR uol.last_name != dim.last_name) and dim.end_date='9999-12-31') -- and dim.end_date='9999-12-31' условие нужно, чтобы не менять историю, даже если на источнике поменялось
       OR dim.customer_id IS NULL;

    BEGIN TRANSACTION;

    UPDATE public.d_customer
    set end_date = '{{ds}}'::date - interval '1 day'
    WHERE id IN (SELECT id
             FROM public.d_customer_stg
             WHERE id IS NOT NULL); --обновляем end_date

    INSERT INTO public.d_customer(customer_id, first_name, last_name, start_date, end_date)
    SELECT stg.customer_id,
       stg.first_name,
       stg.last_name,
       '{{ds}}',
       '9999-12-31'
    FROM public.d_customer_stg stg; -- вставляем актуальное инфо

    INSERT INTO public.d_customer(customer_id, first_name, last_name,start_date, end_date)
    SELECT DISTINCT
        customer_id,
        'неизвестно',
        'неизвестно' ,
       '{{ds}}',
       '9999-12-31'
    FROM public.user_activity_log uol
    LEFT JOIN public.d_customer dim USING (customer_id)
    WHERE dim.id IS NULL; --добавляем клиентов, которые пока не совершали заказов

    COMMIT TRANSACTION;
    """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def load_d_city():
    query = """ 
    WITH
         api_data AS ( --актуальные на дату расчёта данные в источнике
             SELECT DISTINCT
             ON (uol.city_id)
                 uol.city_id,
                 uol.city_name
             FROM public.user_order_log uol
             WHERE
                 uol.date_time::date = '{{ds}}'
             ORDER BY uol.city_id, date_time DESC
    )
    INSERT INTO public.d_city_stg (id, city_id, city_name)
    SELECT --получаем дельту и вставляем данные
           dim.id,
           uol.city_id,
           uol.city_name
    FROM api_data uol
             LEFT JOIN  public.d_city dim on dim.city_id = uol.city_id
             and '{{ds}}' BETWEEN start_date AND end_date
    WHERE (uol.city_name != dim.city_name and dim.end_date='9999-12-31') -- and dim.end_date='9999-12-31' условие нужно, чтобы не менять историю, даже если на источнике поменялось
       OR dim.city_id IS NULL;

         BEGIN TRANSACTION;

          UPDATE public.d_city
          set end_date = '{{ds}}'::date - interval '1 day'
          WHERE id IN (SELECT id
                 FROM public.d_city_stg
                 WHERE id IS NOT NULL); --обновляем end_date

          INSERT INTO public.d_city(city_id, city_name, start_date, end_date)
            SELECT stg.city_id,
               stg.city_name,
               '{{ds}}',
               '9999-12-31'
            FROM public.d_city_stg stg; -- вставляем актуальное инфо

      COMMIT TRANSACTION;
    """  # задаём наш запрос
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:  # подключаемся к БД
        with conn.cursor() as cur:
            cur.execute(query)  # выполняем скрипт

def load_d_item():
    query = """
    WITH
         api_data AS ( --актуальные на дату расчёта данные в источнике
             SELECT DISTINCT
             ON (uol.item_id)
                 uol.item_id,
                 uol.item_name
             FROM public.user_order_log uol
             WHERE
                 uol.date_time::date = '{{ds}}'
             ORDER BY uol.item_id, date_time DESC
    )
    INSERT INTO public.d_item_stg (id, item_id, item_name)
    SELECT --получаем дельту и вставляем данные
           dim.id,
           uol.item_id,
           uol.item_name
    FROM api_data uol
             LEFT join public.d_item dim on dim.item_id = uol.item_id
             and '{{ds}}' BETWEEN start_date AND end_date
    WHERE (uol.item_name != dim.item_name and dim.end_date='9999-12-31') -- and dim.end_date='9999-12-31' условие нужно, чтобы не менять историю, даже если на источнике поменялось
    OR dim.item_id IS NULL;


    BEGIN TRANSACTION;

    UPDATE public.d_item
    set end_date = '{{ds}}'::date - interval '1 day'
    WHERE id IN (SELECT id
                 FROM public.d_item_stg
                 WHERE id IS NOT NULL); --обновляем end_date

    INSERT INTO public.d_item(item_id, item_name, start_date, end_date)
    SELECT stg.item_id,
           stg.item_name,
           '{{ds}}',
           '9999-12-31'
    FROM public.d_item_stg stg; -- вставляем актуальное инфо

    COMMIT TRANSACTION;
    """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
             cur.execute(query)
##############################################################################

def load_f_order():
    query = """
INSERT INTO public.f_order (order_id, create_date, customer_id, city_id, item_id, quantity, payment_amount)
SELECT  DISTINCT ON (uniq_id)
       uniq_id         AS order_id,
       date_time::date AS create_date,
       cus.id      AS customer_id,
       c.id         AS city_id,
       it.id        AS item_id,
       quantity,
       payment_amount
FROM public.user_order_log uol
         JOIN public.d_city c on c.city_id = uol.city_id  and '{{ds}}' BETWEEN c.start_date AND c.end_date
         JOIN public.d_customer cus ON cus.customer_id = uol.customer_id and '{{ds}}' BETWEEN cus.start_date AND cus.end_date
         JOIN public.d_item it on it.item_id = uol.item_id  and '{{ds}}' BETWEEN it.start_date AND it.end_date
WHERE uol.date_time::date = '{{ds}}'
            ORDER BY uniq_id, date_time DESC;
    """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def load_f_activity():
    query = """
    INSERT INTO public.f_activity (activity_id, create_date, customer_id, action_id, quantity)
    SELECT DISTINCT ON (uniq_id)
    uniq_id AS activity_id,
    date_time::date AS create_date,
    cus.id AS customer_id,
    ual.action_id,
    quantity
    FROM public.user_activity_log ual
    JOIN public.d_customer cus 
    ON cus.customer_id = ual.customer_id 
    AND '{{ds}}'::date BETWEEN cus.start_date AND cus.end_date
                WHERE ual.date_time::date = '{{ds}}'::date
                ORDER BY uniq_id, date_time DESC
    """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


with DAG('api_data_load',
         default_args=default_args,
         start_date=datetime(2024, 1, 1),
         schedule_interval='@daily',
         catchup=True,
         max_active_runs=1) as dag:
    order_log = PythonOperator(task_id='order_log',
                               python_callable=insert_user_order_log,
                               provide_context=True)

    activity_log = PythonOperator(task_id='activity_log',
                                  python_callable=insert_user_activity_log,
                                  provide_context=True)

##############################################################################
    d_customer = PythonOperator(task_id='d_customer',
                                python_callable=load_d_customer,
                                provide_context=True)

    d_city = PythonOperator(task_id='d_city',
                            python_callable=load_d_city,
                            provide_context=True)

    d_item = PythonOperator(task_id='d_item',
                            python_callable=load_d_item,
                            provide_context=True)
##############################################################################
  
    f_order = PythonOperator(task_id='f_order',
                             python_callable=load_f_order,
                             provide_context=True)

    f_activity = PythonOperator(task_id='f_activity',
                                python_callable=load_f_activity,
                                provide_context=True)

    (order_log >> activity_log >> d_customer >> d_city >> 
    d_item >> f_order >> f_activity)
