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

def execute_query():
    print("airflow")
    # print("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    keycloak_url = "https://keycloak.hpe-qa9-ezaf.com/realms/UA/protocol/openid-connect/token"
    keycloak_payload = 'username=hpedemo-user04&password=Hpepoc@123&grant_type=password&client_id=ua-grant'
    keycloak_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    print("Keycloak URL: {}".format(keycloak_url))
    response = requests.request("POST", keycloak_url, headers=keycloak_headers, data=keycloak_payload, verify=False)
    print("Create Token API Response code: {}".format(response.status_code))
    print(response.json())
    access_token = response.json()['access_token']
    print(access_token)
    access_token = 'Bearer '+access_token
    ezpresto_url = "http://ezpresto-webservice.ezpresto.svc.cluster.local:8888/api/v1/ezsql"
    ezpresto_payload = json.dumps({"query": "select * from mysql.tpch_partitioned_orc_2.orders"})
    ezpresto_headers = {'Content-Type': 'application/json', 'Authorization': access_token}
    print("EZPresto WebService URL: {}".format(ezpresto_url))
    response = requests.request("POST", ezpresto_url, headers=ezpresto_headers, data=ezpresto_payload, verify=False)
    print("Run Query Response Code: {}".format(response.status_code))
    

# define the DAG
dag = DAG(
    'user01_ezpresto',
    default_args=default_args,
    description='User01 Query',
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
query_task = PythonOperator(
    task_id='query_task',
    python_callable= execute_query,
    dag=dag,
)

query_task