import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.{SparkContext,SparkConf}

import org.apache.spark.sql.Row
/****** WORKING***
  * Created by kiran on 2/14/17.
  * Scenario: Read csv data (a column having comma & enclosed by "), write the data to HDFS by partition year & month
  *
  * Input Data format: 4 columns
  * qdate,id,mode,requoted
  * 6/8/18,111,"100.00, 118.00, 150.00, 174.00",1
  * 7/1/18,"222",300,2
  */
object SparkReadCSVDateOps {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val csvPath = //"/home/kiran/km/km_big_data/data/data_txns_aia.csv"
      "C:\\km\\km_big_data\\data"

    //Read directly as csv with header
    print("---Reading csv directly...")
    val fileOptions1 = Map(("header" -> "true"), ("delimiter" -> ",")) //, ("inferSchema", "false") )
    val dataDF = spark.
      read.
      options(fileOptions1).
      csv(csvPath+"\\data_txns_aia.csv")

    dataDF.printSchema()
    dataDF.show()

    import spark.implicits._
    //import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val ymPrtnDF = dataDF.withColumn("date_dt", to_date($"qdate", "MM/dd/yy")).
      withColumn("year", year($"date_dt")).
      withColumn("month", month($"date_dt")).
      drop("qdate")

    ymPrtnDF.show()

    ymPrtnDF.coalesce(1).
      write.format("csv").option("header", "true").
      option("delimiter","|"). //For comma delimiter, writes data having commas is enclosed by "
      partitionBy("year", "month").
      mode("append").save(csvPath)


  }
}
