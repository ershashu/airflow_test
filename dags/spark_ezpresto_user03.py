from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "spark_ezpresto_user03",
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=["e2e example", "ezaf", "spark", "ezpresto", "local-s3"],
    params={
        "username": Param(
            "hpedemo-user03",
            type="string",
            description="username",
        ),
        "query": Param(
            "select \* from mysql.tpch_partitioned_orc_2.lineitem limit 100", type="string", description="EzPresto Query "
        ),
        "airgap_registry_url": Param(
            "lr1-bd-harbor-registry.mip.storage.hpecorp.net/ezua/",
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="Airgap registry url. Trailing slash in the end is required",
        )
    },
    render_template_as_native_obj=True,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
)

run_ezpresto_query_via_spark = SparkKubernetesOperator(
    task_id="run_ezpresto_query_via_spark",
    application_file="spark_ezpresto_user03.yaml",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
)

sensor_for_run_query_via_spark = SparkKubernetesSensor(
    task_id="sensor_for_run_query_via_spark",
    application_name="{{ task_instance.xcom_pull(task_ids='run_ezpresto_query_via_spark')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
)

run_ezpresto_query_via_spark >> sensor_for_run_query_via_spark