from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from sql.queries import d_customer_query, d_customer_query_stg, d_item_stg_query, d_item_query, d_city_stg_query, \
    d_city_query, f_order_query, f_activity_query, customer_report_query
from utils.parsing_utils import get_insert_user_activity_log, get_insert_user_order_log
from utils.postgres_utils import execute_postgres_query

default_args = {
    'owner': 'airflow',
    'concurrency': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG('api_data_load',
         default_args=default_args,
         start_date=datetime(2024, 1, 1),
         schedule_interval='@daily',
         catchup=True,
         max_active_runs=1) as dag:
    clean_order_log = PythonOperator(
        task_id='clean_order_log',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': """
                                DELETE FROM public.user_order_log
                                WHERE date_time::date='{{ ds }}'"""},  # новый оператор с запросом
    )

    order_log = PythonOperator(
        task_id='order_log',
        python_callable=get_insert_user_order_log,
        provide_context=True,
    )

    clean_activity_log = PythonOperator(
        task_id='clean_activity_log',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': """
                                DELETE FROM public.user_activity_log
                                WHERE date_time::date='{{ ds }}'"""},  # новый оператор с запросом
    )

    activity_log = PythonOperator(
        task_id='activity_log',
        python_callable=get_insert_user_activity_log,
        provide_context=True,
    )
    d_customer_stg = PythonOperator(
        task_id='d_customer_stg',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_customer_query_stg}
    )

    d_customer = PythonOperator(
        task_id='load_d_customer',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_customer_query}
    )

    d_city_stg = PythonOperator(
        task_id='d_city_stg',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_city_stg_query}
    )

    d_city = PythonOperator(
        task_id='load_d_city',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_city_query}
    )
    d_item_stg = PythonOperator(
        task_id='d_item_stg',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_item_stg_query}
    )

    d_item = PythonOperator(
        task_id='load_d_item',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': d_item_query}
    )
    clean_f_order = PythonOperator(
        task_id='clean_f_order',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': """
                                DELETE FROM public.f_order
                                WHERE create_date::date='{{ ds }}'"""},
    )

    f_order = PythonOperator(
        task_id='load_f_order',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': f_order_query}
    )

    clean_f_activity = PythonOperator(
        task_id='clean_f_activity',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': """
                                DELETE FROM public.f_activity
                                WHERE create_date::date='{{ ds }}'"""},  # новый оператор с запросом
        )

    f_activity = PythonOperator(
        task_id='load_f_activity',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': f_activity_query}
    )

    customer_report = PythonOperator(
        task_id='load_customer_report',
        python_callable=execute_postgres_query,
        provide_context=True,
        op_kwargs={'query': customer_report_query}
    )

    (clean_order_log >> order_log >> clean_activity_log >> activity_log >> d_customer_stg >>
     d_customer >> d_city_stg >> d_city >> d_item_stg >> d_item >>
     clean_f_order >> f_order >> clean_f_activity >> f_activity >> customer_report)  # добавляем новые таски