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
from airflow.providers.cncf.kubernetes.sensors.pod import (
    KubernetesPodSensor,
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
    "user01_spark_ezpresto_s3",
    default_args=default_args,
    schedule_interval=None,
    tags=["e2e example", "ezaf", "spark", "ezpresto", "local-s3"],
    params={
        "username": Param(
            "user01",
            type="string",
            description="username",
        ),
        "s3_endpoint": Param(
            "local-s3-service.ezdata-system.svc.cluster.local:30000",
            type="string",
            description="S3 endpoint to push data",
        ),
        "s3_endpoint_ssl_enabled": Param(
            False, type="boolean", description="Whether to use SSL for S3 endpoint"
        ),
        "s3_bucket": Param(
            "ezaf-presto", type="string", description="S3 bucket to push csv data from"
        ),
        "query": Param(
            "query", type="string", description="EzPresto Query "
        ),
        "airgap_registry_url": Param(
            "",
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="Airgap registry url. Trailing slash in the end is required",
        ),
    },
    render_template_as_native_obj=True,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
)

run_ezpresto_query_via_spark = SparkKubernetesOperator(
    task_id="run_ezpresto_query_via_spark",
    application_file="user01_spark_ezpresto.yaml",
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

upload_query_result_to_s3 = KubernetesPodOperator(
    task_id="upload_query_result_to_s3",
    name="hello-dry-run",
    image="lr1-bd-harbor-registry.mip.storage.hpecorp.net/ezua/gcr.io/mapr-252711/kubeflow/notebooks/jupyter-tensorflow-full:ezaf-fy24-q1-r5",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},    
    do_xcom_push=True,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)

sensor_upload_query_result_to_s3 = KubernetesPodSensor(
    task_id="sensor_upload_query_result_to_s3_via_spark",
    application_name="{{ task_instance.xcom_pull(task_ids='upload_query_result_to_s3')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
)


run_ezpresto_query_via_spark >> sensor_for_run_query_via_spark >> upload_query_result_to_s3 >> sensor_upload_query_result_to_s3