from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import requests
import json

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", schedule="@hourly") as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
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

    # Set dependencies between tasks
    hello >> airflow()