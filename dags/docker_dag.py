from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
#http://www.pythonblackhole.com/blog/article/50879/47a8e8f879d6db82b5d2/ - to solve sock.connect error

default_args = {
'owner'                 : 'airflow',
'description'           : 'Use of the DockerOperator',
'depend_on_past'        : False,
'start_date'            : datetime(2023, 8, 13),
'email_on_failure'      : False,
'email_on_retry'        : False,
'retries'               : 1,
'retry_delay'           : timedelta(minutes=5)
}

with DAG('docker_dag', default_args=default_args, schedule_interval="5 * * * *", catchup=False) as dag:
    t1 = BashOperator(
    task_id='print_current_date',
    bash_command='date'
    )

    t2 = DockerOperator(
    task_id='docker_command',
    image='centos:latest',
    api_version='auto',
    auto_remove=True,
    environment={
    'AF_EXECUTION_DATE': "{{ ds }}",
    'AF_OWNER': "{{ task.owner }}"
    },
    command='/bin/bash -c \'echo "TASK ID (from macros): {{ task.task_id }} - EXECUTION DATE (from env vars): $AF_EXECUTION_DATE"\'',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'   
    )

    t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello world"'
    )

    t1 >> t2 >> t3