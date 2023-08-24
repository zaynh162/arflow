import boto3
from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Redshift')
    spark: SparkSession = SparkSession.builder.getOrCreate()

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
    #   spark: SparkSession = SparkSession.builder.config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:3.3.3").master("local[*]").appName(
    #       "SparkByExamples.com").getOrCreate()
    #
    df = spark.read.options(header='True', inferSchema='True').csv("s3://glue-zayn/bootcamp/orders/orders.csv")
    df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://default.354294757369.ap-south-1.redshift-serverless.amazonaws.com:5439/dev") \
    .option("dbtable", "dev.test.orders") \
    .option("driver","com.amazon.redshift.jdbc42.Driver") \
    .option("user", "admin").option("password", "Admin1234") \
    .option("aws_iam_role", "arn:aws:iam::354294757369:role/Redshift_admin") \
    .mode("overwrite")\
    .save()