package com.npntraining


import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkReadJSON2 {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example").master("local")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._


  val df = spark.read.option("multiline", "true").json("C:\\Users\\Manju\\Downloads\\sample_json.json")
    val df2 = df.select("results.gender","results.name.first","results.name.last","results.email")
    
    df2.show()
     
  }
}                                                                                