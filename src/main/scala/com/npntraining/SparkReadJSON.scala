package com.npntraining

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkReadJSON {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example").master("local")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

spark.sqlContext.sql("CREATE TEMPORARY VIEW user USING json OPTIONS" +  " (path 'C:/Users/Manju/Downloads/sample_json.json')")
spark.sqlContext.sql("select results.gender,results.name.first,results.name.last,results.email from user").show(false)


  
    
  }
}                                                                                