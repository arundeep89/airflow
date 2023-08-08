from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
"owner": "Airflow",
"start_date": datetime(2023, 8, 6),
"depends_on_past": False,
"email_on_failure": False,
"email_on_retry": False,
"retries": 1
}

with DAG(dag_id="var_template_macro", schedule_interval='@daily',  catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
    task_id="t1",
    bash_command="echo {{ var.value.source_path }}",
    )
    
    t2 = BashOperator(
    task_id="t2",
    bash_command="echo {{ params.my_param }}",
    params={"my_param": "Hello world"}
    )

    t3 = BashOperator(
    task_id="t3",
    bash_command="echo 'execution date : {{ ds }} modified by macros.ds_add to add 5 days : {{ macros.ds_add(ds, 5) }}'"
    )


    t1 >> t2 >> t3