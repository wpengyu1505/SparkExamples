from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Loader") \
    .getOrCreate()

schema = StructType([
    StructField("col1", IntegerType()),
    StructField("col2", StringType()),
    StructField("col3", StringType())
])

# Read csv
df = spark.read \
    .format("com.databricks.spark.csv") \
    .schema(schema) \
    .option("header", "false") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS") \
    .load("test.csv")

df.write.mode("overwrite").csv("testout")

# Read xml
df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "hierarchy") \
    .option("rowTag", "att") \
    .load("test.xml")



db_properties={}

db_url = "jdbc:postgresql://localhost:5432/test"
db_properties['user'] = "postgres"
db_properties['password'] = "123"
db_properties['url'] = "jdbc:postgresql://localhost:5432/test"
db_properties['driver'] = "org.postgresql.Driver"

df.write.jdbc(url=db_url,table='dummy',mode='append',properties=db_properties)
