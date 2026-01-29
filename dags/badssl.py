from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def test_ca_cert():
    print("Testing HTTPS with custom CA")

    # This will use system CA trust (or CURL_CA_BUNDLE / REQUESTS_CA_BUNDLE if set)
    response = requests.get("https://untrusted-root.badssl.com/", timeout=30)
    print("Status code:", response.status_code)

with DAG(
    dag_id="ca_cert_test_python_operator",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
) as dag:

    ca_cert_test = PythonOperator(
        task_id="ca_cert_test",
        python_callable=test_ca_cert,
    )
