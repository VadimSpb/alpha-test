from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing


def postgres_consumer(postgres_conn_id, target_schema, target_table, sc_installation='na'):

    def _postgres_consumer(in_fh, **kwargs):
        print('---start consumer: postgres_consumer')
        target_tmp_table_name = f'''{target_table}_{sc_installation}_{kwargs.get('ds_nodash')}_tmp'''
        db_connection = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
        with closing(db_connection.cursor()) as db_cursor:
            db_cursor.execute(f'''
                select column_name
                from information_schema.columns
                where table_schema='{target_schema}'
                  and table_name='{target_tmp_table_name}'
            ''')
            columns = db_cursor.fetchall()

            column_list = [c[0] for c in columns]

            if column_list[-1] == 'record_extracted_at':  # gets default value of now()
                column_list.pop(-1)

            load_sql = f'''
                copy {target_schema}.{target_tmp_table_name}
                ({','.join(column_list)})
                from stdin
                with (
                  format csv,
                  delimiter '^',
                  null '',
                  header true
                );
            '''

            db_cursor.copy_expert(sql=load_sql, file=in_fh)

        db_connection.commit()
        db_connection.close()

        print('---finish consumer: postgres_consumer')

    return _postgres_consumer
