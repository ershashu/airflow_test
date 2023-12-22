from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.utils.dates import days_ago
from airflow.models.dagrun import DagRun
import urllib3
import logging
import requests
import json
import sys

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

log = logging.getLogger(__name__)
PATH_TO_PYTHON_BINARY = sys.executable

dag = DAG(
    'user01_ezpresto',
    default_args=default_args,
    schedule_interval='@hourly',
    tags=['ezaf', 'ezpresto'],
    params={
        'ezua_domain': Param("hpe-qa9-ezaf.com", type="string"),
        'query': Param("select * from mysql.tpch_partitioned_orc_2.region", type="string"),
        'ezua_username': Param("hpedemo-user01", type="string"),
        'ezua_password': Param("Hpepoc@123", type="string")
    },
    render_template_as_native_obj=True,
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)


def create_token(**kwargs):
    # logging.info("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.info("Host name: {}".format(DagRun.conf['ezua_domain']))
    keycloak_url = "https://keycloak.{}.com/realms/UA/protocol/openid-connect/token".format(DagRun.conf['ezua_domain'])
    payload = 'username={}&password={}&grant_type=password&client_id=ua-grant'.format(DagRun.conf['ezua_username'], DagRun.conf['ezua_password'])
    keycloak_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    logging.info("Keycloak URL: {}".format(keycloak_url))
    response = requests.request("POST", keycloak_url, headers=keycloak_headers, data=payload)
    logging.info("Create Token API Response code: {}".format(response.status_code))
    logging.info(response.json())
    access_token = response.json()['access_token']
    logging.info(response.json())


create_token_task = PythonOperator(
    task_id="create_token",
    python_callable=create_token,
    dag=dag
)

create_token_task
