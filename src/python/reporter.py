from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import SparkSession

# Compare and check is commission rate is in relation with sales result
def collect_report(spark, df, venues):
    df.createOrReplaceTempView("sales")
    venues.createOrReplaceTempView("venues")
    return spark.sql("""
        select col1, col2 from table
        """)

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

df = spark.read.jdbc(url=db_url,table='sales', properties=db_properties)
venues = spark.read.jdbc(url=db_url,table='venues', properties=db_properties)

out = collect_report(spark, df, venues)
saveResultToCsv(out, "testdbout")
