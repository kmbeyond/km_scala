import org.apache.spark.sql.SparkSession

/*
  //Date ops

  */
object SparkDateOps {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val dfData = List(("1", "6/8/18"), ("2", "7/1/18") )
      .toDF("id", "qdate")

    dfData.show()

    import spark.implicits._
    //import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val dtFromFormat = new java.text.SimpleDateFormat("MM/dd/yy")
    val dtToFormat = new java.text.SimpleDateFormat("YYYY-MM-dd")
    val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    val ymPrtnDF = dfData.
      //withColumn("date_dt", date_format(to_date($"qdate", "MM/dd/yy"), "YYYY-MM-dd")). //Syntax error with IntelliJ
      //withColumn("date_dt", lit(dtFromFormat.format($"qdate"))).
      withColumn("date_dt_l", unix_timestamp($"qdate", "MM/dd/yy")).
      withColumn("date_dt", from_unixtime(unix_timestamp($"qdate", "MM/dd/yy"), "YYYY-MM-dd")).
      withColumn("year", year($"date_dt")).
      withColumn("month", month($"date_dt"))
      //drop("qdate")

    ymPrtnDF.show(false)
/*
    ymPrtnDF.coalesce(1).
      write.format("csv").option("header", "true").
      option("delimiter","|"). //For comma delimiter, writes data having commas is enclosed by "
      partitionBy("year", "month").
      mode("append").save(csvPath+"write_aia")
*/

  }
}
