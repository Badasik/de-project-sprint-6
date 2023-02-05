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


def run_sql_file(path: str):
    logging.info('Start')
    script_name = path
    conn = vertica_python.connect(**conn_info)   
    cur = conn.cursor()
    cur.execute(open(script_name, 'r').read())
    conn.commit()
    
    logging.info('Finish')

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_load():
    start = DummyOperator(task_id = 'Start')

    load_l_user_activity_group = PythonOperator(task_id='load_l_user_activity_group',
                                                python_callable=run_sql_file,
                                                op_kwargs={'path': '/src/sql/load_l_user_group_activity.sql'})
    
    load_s_auth_history = PythonOperator(task_id='load_s_auth_history',
                                                python_callable=run_sql_file,
                                                op_kwargs={'path': '/src/sql/load_s_auth_history.sql'})

    end = DummyOperator(task_id="end")
 
    start >> load_l_user_activity_group >> load_s_auth_history >> end 

dag = sprint6_dag_load()