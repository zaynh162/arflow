try:
    import logging
    from datetime import timedelta
    from airflow import DAG

    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator

    from datetime import datetime

    # Setting up Triggers
    from airflow.utils.trigger_rule import TriggerRule
    import airflow.hooks.S3_hook

    print("All Dag modules are ok ......")
    logging.info("All Dag modules are ok ......")

except Exception as e:
    print("Error  {} ".format(e))


def first_function(**context):
    logging.info("Hello world this works ")


def on_failure_callback(context):
    logging.info("Fail works  !  ")


with DAG(dag_id="single_function_dag",
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

    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
    )

first_function
