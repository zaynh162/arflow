
from pyspark.sql import SparkSession
if __name__ == '__main__':

    spark: SparkSession = SparkSession.builder.master("spark://3b4ec1393204:7077").appName("SparkByExamples.com").getOrCreate()

    # Create RDD from parallelize
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    print("total records :"+str(rdd.count()))
