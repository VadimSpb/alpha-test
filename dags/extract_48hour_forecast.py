from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from castom_etl_lib.openweathermap_provider import openweathermap_provider

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
bronze_path = '../bronze/data.json'

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

for city in cities:
    extract_node = PythonOperator(
        task_id=f"extract_task_{city.replace(' ','_')}",
        python_callable=openweathermap_provider,
        op_kwargs={
            'city_name': city,
            'appid': openweathermap_access_token,
            'file_path': bronze_path,
        },
        dag=dag,
    )

    # transform_node = PythonOperator(
    #     task_id=f"transform_task_{city.replace(' ','_')}",
    #     python_callable=openweathermap_provider,
    #     op_kwargs={
    #         'city': city,
    #         'appid': openweathermap_access_token,
    #         'file_path': bronze_path,
    #     },
    #     dag=dag,
    # )
    #
    # load_node = PythonOperator(
    #     task_id=f"load_task_{city.replace(' ','_')}",
    #     python_callable=openweathermap_provider,
    #     op_kwargs={
    #         'city': city,
    #         'appid': openweathermap_access_token,
    #         'file_path': bronze_path,
    #     },
    #     dag=dag,
    # )

    # extract_node >> transform_node >> load_node