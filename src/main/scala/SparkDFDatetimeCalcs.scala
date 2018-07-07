/**
  * Created by kiran on 2/2/17.
  * This program will round the long datetimemillis to nearest given seconds
  * * Is used to group the data at seconds level
  */

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDFDatetimeCalcs {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Intellij Datetime")
      .set("spark.executor.memory", "2g")
    //.set("spark.driver.allowMultipleContexts", "true")

    val spark = SparkSession.builder().config(conf)
      .getOrCreate()

    //
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

/*
    val sc = new SparkContext(conf)
    case class TS ( ts: java.sql.Timestamp )
    val tsRDD = sc.parallelize(1 to 10).map( x => TS(new java.sql.Timestamp(1001)))
    val df = tsRDD.toDF()
    df.selectExpr("cast(ts as long)").show
    df.selectExpr("cast(ts as long)").show
*/
    val currentTime = System.currentTimeMillis()
    //println("Current Time=" + currentTime.cast("double"))
  val grpSeconds = 9

    val linesDF = List(("1", 1525172806269L), ("2", 1525172797440L) )
      .toDF("Id", "time_ms")
    linesDF.show()
    //linesDF.printSchema()

    linesDF.withColumn("time_ms_dt", (($"time_ms".cast("float")/1000).cast("timestamp")))
      .show(false)



    val linesDFDTR = linesDF.withColumn("time_ms_dt_seconds_rounded", ((($"time_ms".cast("float")/1000)- ($"time_ms".cast("float")/1000) % 9)).cast("timestamp"))
    linesDFDTR.show(false)

    //linesDFDTR.printSchema()


    val linesDFDTR2 = linesDFDTR.withColumn("time_ms_dt_seconds_rounded2", ($"time_ms_dt_seconds_rounded".cast("float")*1000).cast("long"))
    linesDFDTR2.show(false)

    //Final
    val linesDF2 = linesDF.withColumn("time_ms_dt_seconds_rounded", ((($"time_ms".cast("float")/1000)- ($"time_ms".cast("float")/1000) % grpSeconds).cast("float")*1000).cast("long"))
    linesDF2.show(false)
  }
}

