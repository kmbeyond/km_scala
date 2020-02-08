package com.wp.da.csig

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
//import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object filter_by_max_column {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("KM Spark Session")
      .enableHiveSupport()
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._
    import spark.implicits._

    val dataDF = Seq(("v1", "2019-01-01"), ("v2", "2019-01-02"), ("v1", "2019-01-02")).
      toDF("val", "extract_date")


    //--------Option#1: Using Window: --------- BUT GIVING PREVIOUS DAY PARTITION -----
    import org.apache.spark.sql.functions.{add_months, date_add, _}
    import org.apache.spark.sql.expressions.Window

    val win_extract_date = Window.partitionBy("extract_date")

    dataDF.
      filter($"extract_date".gt(date_add(current_date,-7))).
      withColumn("max_extract_date", max($"extract_date").over(win_extract_date)).
      filter($"extract_date" === $"max_extract_date").
      show(false)


    //-------Option#2: Get max value from separate query
    val mx_extract_date = dataDF.agg(max("extract_date")).collect().head.getString(0)

    dataDF.
      filter($"extract_date" === lit(mx_extract_date)).
      show(false)

}
}
