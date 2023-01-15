import requests
import math
import json
import datetime
import airflow
import random
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.fs_hook import FSHook

DEFAULT_ARGS ={
    'owner':'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'concurrency':0,
    'retries':0,
    'schedule_interval': datetime.timedelta(minutes=1)

}

filtering_dag = DAG(
    dag_id='filtering_dag', # name of dag
    default_args=DEFAULT_ARGS, # args assigned to all operators
)

def download_json():
    '''
    Downloads the json file from UTCloud.
    If the file already exists, then it is NOT downloaded. 
    '''
    from os.path import exists
    url = "https://owncloud.ut.ee/owncloud/s/4rTHn9Ex9KXQwkM/download/publications20k.json"
    output_filename = "/opt/airflow/dags/publications.json"
    if(not exists(output_filename)):
        r = requests.get(url)
        with open(output_filename, 'wb') as f:
            f.write(r.content)


first_task = PythonOperator(
    task_id='download_json_file',
    dag=filtering_dag,
    trigger_rule='none_failed',
    python_callable=download_json,
)


def clean_the_data():

    filename = "/opt/airflow/dags/publications.json"
    output_filename = "/opt/airflow/dags/publications.json"

    data = pd.read_json(filename, orient="index") 
    try:
        data = data.drop(columns=["id", "abstract", "update_date", "license"])
    except:
        pass

    data = data[[len(i) >= 35 for i in data['title']]]


    data = data[~data.drop(columns=["versions", "authors_parsed"]).duplicated(keep=False)]

    data.dropna(subset=['authors'])

    data = data.reset_index(drop=False)
    data = data.rename(columns={'index':'id'})
    
    data.to_json(output_filename, orient="index")


second_task = PythonOperator(
    task_id='clean_the_data',
    dag=filtering_dag,
    trigger_rule='none_failed',
    python_callable=clean_the_data,
)

first_task >> second_task

def transform_the_data():
    filename = "/opt/airflow/dags/publications.json"
    output_filename = "/opt/airflow/dags/publications.json"

    data = pd.read_json(filename, orient="index")

    def publ_created(row):
        return row["versions"][0]['created']

    def publ_lastupdate(row):
        return row["versions"][-1]['created']

    data['publ_date'] = data.apply(publ_created, axis=1)
    data['publ_lastupdate'] = data.apply(publ_lastupdate, axis=1)

    data.to_json(output_filename, orient="index")

third_task = PythonOperator(
    task_id='transform_the_data',
    dag=filtering_dag,
    trigger_rule='none_failed',
    python_callable=transform_the_data,
)

second_task >> third_task