from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from castom_etl_lib.openweathermap_provider import openweathermap_provider
from castom_etl_lib.json_to_csv_transformer import process_json_to_csv
from castom_etl_lib.postgres_consumer import postgres_consumer

description = ''' 
    This DAG extracts hourly weather forecast for 48 hours taken from www.openweathermap.org
    '''

cities = [
    'Moscow',
    'Kazan',
    'Saint Petersburg',
    'Tula',
    'Novosibirsk',
]

default_args = {
    'owner': 'Vadim M.',
    'email': ['v.mazeyko@gmail.com'],
    'email_on_failure': False,
}

postgres_conn_id = 'postgres_local'
openweathermap_access_token = Variable.get("openweathermap_access_token")
target_schema = 'public'
target_table = 'weather_forecasts'

dag = DAG(
    dag_id='extract_48hour_forecast',
    description=description,
    default_args=default_args,
    start_date=datetime(2023, 12, 7),
    schedule_interval='0 12 * * *',
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    tags=['weather_forecast']
)

for city_name in cities:
    extract_node = PythonOperator(
        task_id=f"extract_task_{city_name.replace(' ','_')}",
        python_callable=openweathermap_provider,
        op_kwargs={
            'city_name': city_name,
            'appid': openweathermap_access_token,
        },
        dag=dag,
    )

    transform_node = PythonOperator(
        task_id=f"transform_task_{city_name.replace(' ','_')}",
        python_callable=process_json_to_csv,
        op_kwargs={
            'city_name': city_name,
        },
        dag=dag,
    )

    load_node = PythonOperator(
        task_id=f"load_task_{city_name.replace(' ','_')}",
        python_callable=postgres_consumer,
        op_kwargs={
            'city_name': city_name,
            'postgres_conn_id': postgres_conn_id,
            'target_schema': target_schema,
            'target_table': target_table,
        },
        dag=dag,
    )

    # extract_node >> transform_node >> load_node
