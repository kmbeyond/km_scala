package com.kiran.kafka

//import java.util
//import java.{util => ju}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkDateTest2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KM Test")
      .setMaster("local[4]")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    //val sc = new SparkContext(conf)
    //sc.setLogLevel("INFO")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate
    val sc = spark.sparkContext

    import spark.implicits._

    val statsRefreshedTimestamp = System.currentTimeMillis()

    val genre = sc.parallelize(List(("id1", "2016-05-01", "action",0),
      ("id1", "2016-05-03", "horror",1),
      ("id2", "2016-05-03", "art",0),
      ("id2", "2016-05-04", "action",0),
      ("id1", "2016-05-04", "horror",1),
        ("", "2016-05-04", "action",0)
    )).
      toDF("id","date","genre","score")

    genre.show(false)

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions.{lit, when, col}
    val validGenres = genre.withColumn("date", when(col("id") === lit(""), "0000-00-00").otherwise(col("date")) )
      //.filter($"date" =!= "0000-00-00")
    validGenres.show(false)
    validGenres.printSchema()

    //case class Genre(id:String, date:String, genre:String, score:Integer)
    //validGenres.map(r => { Genre(r.get(0).asInstanceOf[String],
    //                            r.get(1).asInstanceOf[String],
    //                            r.get(2).asInstanceOf[String],
    //                            r.get(3).asInstanceOf[Integer]) })

    //validGenres.rdd.foreach(println)

      //.toDF("A", "B", "C", "D").show(false)
    val processedGenres = validGenres.rdd
      .map(r => { (r.get(1), r.get(0)+","+r.get(2)+","+r.get(3) ) } )
      .groupBy(r => r._1.toString())
      .mapValues(r => (r.toList))
      .groupByKey()
      .foreach(println)


    //processedGenres.foreach(r => println(r))
        //.toDF("A", "B").show(false) //CAN'T CONVERT TO DF
      //.collect.foreach(println)
      //.toDF("AA", "BB").show(false);



    println("Exit")
  }
    def getDT()
    : String = {

      val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
      return dateFormatter.format(new Date())

      //val today = Calendar.getInstance.getTime
      //return dateFormatter.format(today)
    }
}
