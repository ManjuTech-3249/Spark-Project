package com.npntraining

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{ SaveMode, SparkSession }

object MoveFile {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);

    val path = "H:\\Big Data\\Spark Project\\CSV files"
    //watchDirectory(path)
    
    val spark = SparkSession
      .builder
      .appName("Moving file")
      .master("local")
      .getOrCreate()
      
    val df = spark.read.parquet("D:\\part-00000-3aae0bf2-2d21-4cac-b624-68d94708e70f.c000.snappy.parquet")
    
    df.show(10)

  }

  def watchDirectory(path: String): Unit = {

    val faxFolder: Path = Paths.get(path)
    val watchService: WatchService = FileSystems.getDefault().newWatchService();
    faxFolder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

    var valid: Boolean = true;
    do {
      val watchKey: WatchKey = watchService.take();

      watchKey.pollEvents().asScala.foreach(event => {
        val kind = event.kind

        if (StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind())) {
          val filename: String = event.context().toString()
          println(filename)
          if (filename.endsWith(".csv")) {

            val file_path: String = path + "\\" + filename

            csvToParquet(file_path)
          }
        }
      })
      valid = watchKey.reset()
    } while (valid)
  }
  def csvToParquet(File_Path: String): Unit = {

    println(File_Path)

    val conf = new SparkConf().set("spark.local.dir", "H:\\Big Data\\Spark Project\\Temp\\")

    val spark = SparkSession
      .builder
      .appName("Moving file")
      .master("local")
      .config(conf)
      .getOrCreate()

    val schema = new StructType()
      .add("name", DataTypes.StringType)
      .add("age", DataTypes.IntegerType)
      .add("city", DataTypes.StringType)
      .add("country", DataTypes.StringType)

    val df = spark.read.format("CSV")
      .option("header", "true")
      .schema(schema)
      .load(File_Path)

    df.write.partitionBy("city").mode(SaveMode.Append)
      .parquet("H:\\Big Data\\Spark Project\\Parquet files\\")

    spark.close()
  }
}