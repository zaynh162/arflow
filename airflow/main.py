# This is a sample Python script.
from pyspark import SparkConf
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark: SparkSession = SparkSession.builder.getOrCreate()

    # Create RDD from parallelize
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    print("total records :"+str(rdd.count()))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
