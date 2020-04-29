package com.npntraining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkJSONReader {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "simple-producer-consumer")
      .option("failOnDataLoss", "false")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("name", DataTypes.StringType)
      .add("age", DataTypes.IntegerType)
      .add("city", DataTypes.StringType)
      .add("country", DataTypes.StringType)

    val df2 = df1.select(from_json(col("value"), schema).as("person"))
      .selectExpr("person.*")

    val CSVDF = df2.writeStream
      .format("csv")
      .option("header", "true")
      .option("path", "H:\\Big Data\\Spark Project\\CSV files")
      .option("checkpointLocation", "H:\\Big Data\\Spark Project\\Checkpoint")
      .start()
      .awaitTermination()

  }

}