import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.bash import BashSensor
from airflow.sensors.filesystem import FileSensor
from sensors.custom_sensor import MyCustomFileSensor


@task
def final_task():
    return json.dumps({'return': 'i am done'})


with DAG(
    dag_id="sensor_dag",
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

    
    is_file_available = FileSensor(
        task_id="is_file_available",
        fs_conn_id="forex_path",
        filepath="abc.txt",
        poke_interval =5,
        timeout=20
    )

    end_task = final_task()

    sleep_10sec_sensor >> check_file >> is_file_available >> end_task