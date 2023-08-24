try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
except Exception as e:
    print("import ok")


def hello_display():
    print("hello airflow")


def on_failure_callback(context):
    print("Fail works  !  ")


with DAG(dag_id="hello_world_dag",
         schedule_interval="@once",
         default_args={
             "owner": "airflow",
             "start_date": datetime(2020, 11, 1),
             "retries": 1,
             "retry_delay": timedelta(minutes=1),
             'on_failure_callback': on_failure_callback,
             'email': ['zaynh162@gmail.com'],
             'email_on_failure': False,
             'email_on_retry': False,
         },
         catchup=False) as dag:
    hello_airflow = PythonOperator(
        task_id="hello_airflow",
        python_callable=hello_display
    )

hello_airflow
