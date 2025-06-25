from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.python import get_current_context
import time

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

with DAG(
        dag_id='starving_dag',
        default_args=args,
        schedule_interval=None,
        catchup=False,
        tags=['example', 'performance_benchmarking', 'parallel_tasks'],
        params={
            'count': Param(20, type="integer", minimum=0),
            'sleep': Param(120, type="integer", minimum=0),
        },
        render_template_as_native_obj=True,
        max_active_tasks=1024
) as dag:
    @task
    def get_count_test():
        context = get_current_context()
        return list(range(context['params']['count']))


    run_this_test = BashOperator(
        task_id='run_after_loop_test',
        bash_command='echo test',
        dag=dag,
    )


    @task(pool="test_pool")
    def runme_test(i):
        print(i)
        context = get_current_context()
        time.sleep(context['params']['sleep'])


    runme_test.expand(i=get_count_test()) >> run_this_test