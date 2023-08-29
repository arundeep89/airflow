import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.bash import BashSensor
from sensors.custom_sensor import MyCustomFileSensor


@task
def final_task():
    return json.dumps({'return': 'i am done'})


with DAG(
    dag_id="sensors_dag_demo",
    schedule=None,
    start_date=datetime(2023, 8, 27),
    catchup=False,
) as dag:

    sleep_10sec_sensor = BashSensor(
        task_id='sleep_10sec_sensor',
        poke_interval=2,
        timeout=30,
        soft_fail=False,
        retries=0,
        bash_command="sleep 10",
        dag=dag)

    check_file = MyCustomFileSensor(
        task_id='check_file',
        filepath='/opt/airflow/abc.txt',
        poke_interval=10,
        timeout=60,
        dag=dag
    )

    end_task = final_task()

    sleep_10sec_sensor >> check_file >> end_task