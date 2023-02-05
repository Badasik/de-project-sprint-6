from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum

import boto3



conn_info = {'host': '51.250.75.20', 
             'port': 5433,
             'user': 'BADASOVANTYANDEXRU', # логин       
             'password': 'sxcAyXboOp1vlM4', # пароль
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

bash_command_tmpl = 'head -10 {{ params.files }}'

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_group():
    bucket_files = ['group_log.csv']
    task1 = PythonOperator(
        task_id=f'fetch_files',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'data-bucket', 'keys': bucket_files},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

_ = sprint6_dag_get_group()