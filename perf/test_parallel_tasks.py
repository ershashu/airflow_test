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
    dag_id='example_parallel_tasks',
    default_args=args,
    schedule_interval=None,
    tags=['example', 'performance_benchmarking', 'parallel_tasks'],
    params={
        'count': Param(30, type="integer", minimum=0),
        'sleep': Param(120, type="integer", minimum=0),
    },
    render_template_as_native_obj=True,
    max_active_runs=2048,
    max_active_tasks=2048,
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
) as dag:
    @task
    def get_count():
        context = get_current_context()
        return list(range(context['params']['count']))

    run_this = BashOperator(
        task_id='run_after_loop',
        bash_command='echo test',
        dag=dag,
    )

    @task
    def runme(i):
        print(i)
        context = get_current_context()
        time.sleep(context['params']['sleep'])


    runme.expand(i=get_count()) >> run_this
