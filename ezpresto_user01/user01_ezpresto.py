from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import requests
import json
import urllib3


def airflow_test():
    print("airflow")
    # print("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("Host name: {}".format(DagRun.conf['ezua_domain']))
    keycloak_url = "https://keycloak.hpe-qa9-ezaf.com/realms/UA/protocol/openid-connect/token"
    payload = 'username=hpedemo-user01&password=Hpepoc@123&grant_type=password&client_id=ua-grant'
    keycloak_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    print("Keycloak URL: {}".format(keycloak_url))
    response = requests.request("POST", keycloak_url, headers=keycloak_headers, data=payload)
    print("Create Token API Response code: {}".format(response.status_code))
    print(response.json())
    access_token = response.json()['access_token']
    print(access_token)

# define the DAG
dag = DAG(
    'user01_ezpresto',
    default_args=default_args,
    description='How to use the Python Operator?',
    schedule_interval='@hourly',
    tags=['ezaf', 'ezpresto'],
)

# define the first task
t1 = PythonOperator(
    task_id='print_word_count',
    python_callable= airflow_test,
    dag=dag,
)

t1