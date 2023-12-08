import os
from contextlib import closing
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

def postgres_consumer(city_name, postgres_conn_id, target_schema, target_table):
    print('---start consumer: postgres_consumer')
    db_connection = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    file_path = os.path.join(Variable.get("silver_tier_path"), f"{city_name}.csv")
    with closing(db_connection.cursor()) as db_cursor, open(file_path, 'r') as file:
        load_sql = f'''
                COPY {target_schema}.{target_table}
                FROM stdin
                WITH (
                  FORMAT csv,
                  HEADER true,
                  DELIMITER ','
                );
        '''
        db_cursor.copy_expert(sql=load_sql, file=file)
        db_connection.commit()

        print('---finish consumer: postgres_consumer')