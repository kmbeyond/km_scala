package com.kiran

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by kiran on 2/13/17.
  */
object SparkConfigParameters {
  def main(args: Array[String]) {

    //val conf       = new SparkConf().setAppName("Scala UDF Example")
    //conf.getAll.mkString("\n").foreach(print)

    //Spark Session, No need to create SparkContext, automatically get with SparkSession
    val warehouseLocation = "file:/home/kiran/km_hadoop_fs/warehouse"
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Read config params")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate

    //spark.conf.getAll.mkString("\n").foreach(print)
    //get all settings
    val configMap:Map[String, String] = spark.conf.getAll
    for ((k,v) <- configMap) println(s"key: $k, value: $v")

  }
}
