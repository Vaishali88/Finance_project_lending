from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf_spark = SparkConf().set("spark.driver.host", "127.0.0.1")
spark = SparkSession.builder\
			.appName('demo01')\
			.config('spark.sql.shuffle.partitions', '4') \
	        .master('local') \
	        .getOrCreate()
filepath = "hdfs://localhost:9000/lending_club_project/accepted_2007_to_2018Q4.csv"
df = spark.read \
	.format("csv") \
    .option("header", "true") \
	.option("mode","permissive")\
    .option("inferSchema", "True") \
	.load()
df.printSchema()