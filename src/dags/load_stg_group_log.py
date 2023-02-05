from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import pandas as pd
import boto3
import vertica_python
import logging
from airflow.operators.dummy_operator import DummyOperator

conn_info = {'host': '51.250.75.20', 
             'port': 5433,
             'user': 'BADASOVANTYANDEXRU', # логин       
             'password': 'sxcAyXboOp1vlM4', # пароль
             'database': 'dwh',
             'autocommit': True }

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"


def load_in_stg(key: str, schema: str):
    df_tmp = pd.read_csv(f'/data/{key}.csv')

    logging.info('CSV -> DF complete')

    logging.info('DF_tmp -> CSV')

    df_tmp.to_csv(f'/data/tmp_{key}.csv', index=False)

    logging.info('Sucssed')

    logging.info('Connect to Vertica')

    conn = vertica_python.connect(**conn_info)
    cur = conn.cursor()
    cur.execute(f'TRUNCATE TABLE {schema}.{key}')
    conn.commit()

    logging.info('Table truncated')

    logging.info('Getting columns names')

    cur.execute(f"""SELECT column_name 
                    FROM v_catalog.columns 
                    WHERE table_name = \'{key}\' 
                    AND table_schema = 'BADASOVANTYANDEXRU__STAGING'""")
    column_list = str(cur.fetchall()).replace('[','').replace(']','').replace('\'','')
    
    logging.info(column_list)

    vert_expr = f"""COPY {schema}.{key} ({column_list})
                    FROM LOCAL '/data/tmp_{key}.csv'
                    DELIMITER ','
                    SKIP 1
                    REJECTED DATA AS TABLE 
                    {schema}.{key}_rej
                 """

    logging.info('Start loading')

    cur.execute(vert_expr)
    conn.commit()

    logging.info('Finish loading')


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))

def sprint6_dag_get_data_group_log():
    start = DummyOperator(task_id = 'Start')

    load_groups_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_in_stg,
        op_kwargs={'key': 'group_log', 'schema': 'BADASOVANTYANDEXRU__STAGING'}
    )

    end = DummyOperator(task_id="end")

    start >> load_groups_log >> end

dag = sprint6_dag_get_data_group_log()