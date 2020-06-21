package io.wpengyu.spark.perf

import java.io.FileInputStream
import java.io.PrintWriter
import java.util
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.kohsuke.args4j.{CmdLineParser, Option}

object BucketingExample {

  def main(args:Array[String]): Unit = {
    val dataInput = args(0)
    val metaInput = args(1)
    val numPartitions = 10

    val spark = SparkSession.builder
      .appName("POC")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .config("spark.sql.shuffle.partitions", numPartitions)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .enableHiveSupport().getOrCreate()

    val schema1 = StructType(Array(
      StructField("id", StringType, true),
      StructField("symbol", StringType, true),
      StructField("seq", StringType, true),
      StructField("event", StringType, true)
    ))

    val schema2 = StructType(Array(
      StructField("id", StringType, true),
      StructField("symbol", StringType, true),
      StructField("company", StringType, true)
    ))

    val eventDataNoBucket = spark.read
      .format("com.databricks.spark.csv")
      .option("header", false)
      .option("delimiter", ",")
      .schema(schema1)
      .load(dataInput)

    val metaDataNoBucket = spark.read
      .format("com.databricks.spark.csv")
      .option("header", false)
      .option("delimiter", ",")
      .schema(schema2)
      .load(metaInput)

    saveBucketDataSingle(eventDataNoBucket, metaDataNoBucket, "event_bucketed_single", "meta_bucketed_single")
    saveBucketDataMultiple(eventDataNoBucket, metaDataNoBucket, "event_bucketed_multiple", "meta_bucketed_multiple")

    eventDataNoBucket.write.mode("overwrite").saveAsTable("event_no_bucket")
    metaDataNoBucket.write.mode("overwrite").saveAsTable("meta_no_bucket")

    spark.sql("select a.id, a.symbol, a.seq, a.event, b.company from event_no_bucket a inner join meta_no_bucket b on a.id = b.id").write.mode("overwrite").saveAsTable("result1")
    spark.sql("select a.id, a.symbol, a.seq, a.event, b.company from event_bucketed_single a inner join meta_bucketed_single b on a.id = b.id").write.mode("overwrite").saveAsTable("result2")
    spark.sql("select a.id, a.symbol, a.seq, a.event, b.company from event_bucketed_multiple a inner join meta_bucketed_multiple b on a.id = b.id").write.mode("overwrite").saveAsTable("result3")
    spark.sql("select a.id, a.symbol, a.seq, a.event, b.company from event_bucketed_multiple a inner join meta_bucketed_multiple b on a.id = b.id and a.symbol = b.symbol").write.mode("overwrite").saveAsTable("result4")

    Thread.sleep(10000000)

  }

  def saveBucketDataSingle(eventDataNoBucket:DataFrame,
      metaDataNoBucket:DataFrame,
      dataTable:String,
      metaTable:String) = {

    eventDataNoBucket.write.mode("overwrite")
      .bucketBy(10, "id")
      .saveAsTable(dataTable)

    metaDataNoBucket.write.mode("overwrite")
      .bucketBy(10, "id")
      .saveAsTable(metaTable)
  }

  def saveBucketDataMultiple(eventDataNoBucket:DataFrame,
      metaDataNoBucket:DataFrame,
      dataTable:String,
      metaTable:String) = {

    eventDataNoBucket.write.mode("overwrite")
      .bucketBy(10, "id", "symbol")
      .saveAsTable(dataTable)

    metaDataNoBucket.write.mode("overwrite")
      .bucketBy(10, "id", "symbol")
      .saveAsTable(metaTable)
  }
}