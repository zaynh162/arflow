from pyspark import SparkConf
from pyspark.sql import SparkSession
try:
    import airflow
    from datetime import datetime, timedelta
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator


except Exception as e:
    print("Error  {} ".format(e))
import pyspark
import py4j
print("versions")
print(pyspark.__version__)
print(py4j.__version__)

def processo_etl_spark():
    c = SparkConf()
    c.set("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:3.3.3")
    spark: SparkSession = SparkSession.builder.config(c).master("local[*]").appName(
        "SparkByExamples.com").getOrCreate()

    # Create RDD from parallelize
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    print("total records :" + str(rdd.count()))

default_args = {
    'owner': 'zayn',
    'start_date': datetime(2023, 7, 7),
    'retries': 1,
    'retry_delay': timedelta(hours=1)
}
with airflow.DAG(dag_id="spark_submit_dag",
                 default_args=default_args,
                 schedule_interval="@once") as dag:
    spark_submit = PythonOperator(
        task_id='spark_submit',
        python_callable=processo_etl_spark
    )
spark_submit


