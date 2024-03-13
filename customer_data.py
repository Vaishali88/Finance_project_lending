from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark import sql
from pyspark.sql.functions import col, current_timestamp

conf_spark = SparkConf().set("spark.driver.host", "127.0.0.1")
spark = SparkSession.builder\
			.appName('demo01')\
	        .master('local') \
	        .enableHiveSupport() \
	        .getOrCreate()

filepath = "hdfs://localhost:9000/lending_club_project/customer_data/part-00000-694b65c5-0b8f-4760-ac12-2b59978292e0-c000.csv"

scschema = ("member_id string,emp_title string,emp_length string,home_ownership string,annual_inc float,addr_state string,zip_code string,country string,grade string,sub_grade string,verification_status string,tot_hi_cred_lim float,application_type string,annual_inc_joint float,verification_status_joint string")
df = spark.read.format("csv") \
    .option("inferSchema",True) \
	.option("header",True) \
	.schema(scschema)\
    .load(filepath)


df = df.withColumnRenamed('annual_inc',"annual_income") \
	.withColumnRenamed('addr_state','address_state')\
	.withColumnRenamed('zip_code','address_zip_code')\
	.withColumnRenamed('country','address_country')\
	.withColumnRenamed('tot_hi_cred_lim','total_high_credit_limit')\
	.withColumnRenamed('annual_inc_joint','join_annual_income')
df.printSchema()
customer_df_renamed = df.withColumn("ingest_date",current_timestamp())
customer_df_renamed.printSchema()

customer_distinct = customer_df_renamed.distinct()
customer_distinct.createOrReplaceTempView("customer")
spark.sql("select count(*) from customer where annual_income is null").show()
