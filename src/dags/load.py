from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
from airflow.models import Variable
import logging
import vertica_python
import pandas as pd
import boto3

conn_info = {'host': '51.250.75.20', 
             'port': 5433,
             'user': 'BADASOVANTYANDEXRU', # логин       
             'password': Variable.get("PASSWORD_S3"), # пароль
             'database': 'dwh',
             'autocommit': True }

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"


def fetch_s3_file(bucket: str, keys: list):
    session = boto3.session.Session()
    s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    for key in keys:
        s3_client.download_file(
        Bucket='sprint6',
        Key=key,
        Filename=f'/data/{key}'
    )

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

def run_sql_file(path: str):
    logging.info('Start')
    script_name = path
    conn = vertica_python.connect(**conn_info)   
    cur = conn.cursor()
    cur.execute(open(script_name, 'r').read())
    conn.commit()
    
    logging.info('Finish')

###  
  
@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_load_group_log():
    bucket_files = ['group_log.csv']

    download_csv = PythonOperator(
        task_id=f'fetch_files',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'data-bucket', 'keys': bucket_files},
    )

    load_groups_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_in_stg,
        op_kwargs={'key': 'group_log', 'schema': 'BADASOVANTYANDEXRU__STAGING'}
    )
    
    load_l_user_activity_group = PythonOperator(task_id='load_l_user_activity_group',
                                                python_callable=run_sql_file,
                                                op_kwargs={'path': '/src/sql/load_l_user_group_activity.sql'})
    
    load_s_auth_history = PythonOperator(task_id='load_s_auth_history',
                                                python_callable=run_sql_file,
                                                op_kwargs={'path': '/src/sql/load_s_auth_history.sql'})

    download_csv >> load_groups_log >> load_l_user_activity_group >> load_s_auth_history

dag = sprint6_dag_load_group_log()