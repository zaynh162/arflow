from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark: SparkSession = SparkSession.builder.getOrCreate()
    df = spark.read.options(header='True', inferSchema='True').csv("s3://glue-zayn/bootcamp/orders/orders.csv")
    df.write.parquet("s3://zayn-emr/capstone/airflow_out/order_par1/")