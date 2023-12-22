from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.utils.dates import days_ago
import requests
import json
import urllib3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}

def airflow_test():
    print("airflow")
    # print("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
    render_template_as_native_obj=True,
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

# define the first task
t1 = PythonOperator(
    task_id='print_word_count',
    python_callable= airflow_test,
    dag=dag,
)

t1