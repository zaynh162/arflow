try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")
    context['ti'].xcom_push(key='mykey', value="first_function_execute says Hello ")
    context['ti'].xcom_push(key='k2', value=20)


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name": "zayn", "title": "Full Stack Software Engineer"},
            {"name": "zaynh", "title": "Full Stack Software Engineer"}, ]
    df = pd.DataFrame(data=data)
    print('@' * 66)
    print(df.head())
    print('@' * 66)
    value = context.get("ti").xcom_pull(key="k2")
    print("I am in second_function_execute got value :{} from Function 1  ".format(instance))
    print("value received from first " + str(value))
    context['ti'].xcom_push(key='k3', value=30)

def third_function_execute(**context):
    value = context.get("ti").xcom_pull(key="k3")
    print("value received from second " + str(value))



with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Zanh"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

    third_function_execute = PythonOperator(
        task_id="third_function_execute",
        python_callable=third_function_execute,
        provide_context=True,
    )

first_function_execute >> second_function_execute >> third_function_execute
