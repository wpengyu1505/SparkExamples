from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import SparkSession

# I want to know if sales results in Feb 2020 are better than results in Feb 2019 and I would like to drill it by resellers and event types
def compare_months(spark, df):
    df.createOrReplaceTempView("sales")
    spark.sql("to be filled")

# Compare and check is commission rate is in relation with sales result
def get_comision_rate_with_sales(spark, df):
    df.createOrReplaceTempView("sales")
    spark.sql("to be filled")

# Most popular tickets per regions
def get_most_popular_ticket_per_region(spark, df):
    df.createOrReplaceTempView("sales")
    spark.sql("select date, event_name, sum(num_ticket) from sales group by date, event_name")

def saveResultToCsv(df, filename):
    df.write.mode("overwrite").csv(filename)


spark = SparkSession \
    .builder \
    .appName("Reporter") \
    .getOrCreate()

db_properties={}

db_url = "jdbc:postgresql://localhost:5432/test"
db_properties['user'] = "postgres"
db_properties['password'] = "123"
db_properties['url'] = "jdbc:postgresql://localhost:5432/test"
db_properties['driver'] = "org.postgresql.Driver"

df = spark.read.jdbc(url=db_url,table='dummy', properties=db_properties)
saveResultToCsv(df, "testdbout")

most_popular_df = get_most_popular_ticket_per_region(spark, df)
saveResultToCsv(most_popular_df)

compare_df = compare_months(spark. df)

