from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os

def test_ca_cert():
    print("Testing HTTPS with custom CA")

    ca_path = os.getenv("EZUA_DOMAIN_CA_CERT_PATH")
    print("EZUA_DOMAIN_CA_CERT_PATH:", ca_path)

    response = requests.get(
        "https://untrusted-root.badssl.com/",
        timeout=30,
    )
    print("Status code:", response.status_code)

with DAG(
    dag_id="ca_cert_test_python_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False,
    access_control={"All": {"can_read"}},
) as dag:

    ca_cert_test = PythonOperator(
        task_id="ca_cert_test",
        python_callable=test_ca_cert,
    )
