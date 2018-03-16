import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, Duration}

/**
  * Created by kiran on 3/15/18.
  */
object SparkMeasureTimeLapse {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //Using plain RDD
    println("Read csv ... into RDD")
    var timeStartSec = DateTime.now()
    var timeStart = System.nanoTime()
    val dataRows = sc.textFile("/home/kiran/km/km_big_data/data/data_year_max_temp.csv").
      map(line => line.split(",")) //.map(_.trim))
    val header = dataRows.first
    val data = dataRows.filter(_(0) != header(0))
    val filtered = data.filter(rec => (rec(2) != "9999" && rec(2).matches("[01459]")))
    val tuples = filtered.map(rec => (rec(0).toInt, rec(1).toInt))
    var maxTemps = tuples.reduceByKey((a, b) => Math.max(a, b))
    //maxTemps.foreach(println(_))
    var timeEndSec = DateTime.now()
    var timeEnd = System.nanoTime()
    println("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec")
    println("Elapsed Time (micro sec): "+ ( timeEnd-timeStart)/1e3 +" micro sec")

    //Using DF
    print("Read csv ... into dataframe\n")
    timeStart = System.nanoTime()
    timeStartSec = DateTime.now()
    val schemaTemp =
      StructType(
        StructField("year", IntegerType, false) ::
          StructField("max_temp", IntegerType, false) ::
          StructField("quality", IntegerType, false) :: Nil)
    var dfCSVTxns = spark.read.format("csv").
      options( Map("header" -> "true", "mode" -> "DROPMALFORMED") ).
      schema(schemaTemp).
      load("/home/kiran/km/km_big_data/data/data_year_max_temp.csv")

    import org.apache.spark.sql.functions.{max}
    val dfCSVTxns2 = dfCSVTxns.filter(dfCSVTxns("quality") !== "9999") // && $"quality" !== "NA") //"&&", "and" NOT WORKING
    val dfCSVTxns3 = dfCSVTxns2.groupBy(dfCSVTxns2("year")).agg(max("max_temp").alias("max_temp"))

    //import org.apache.spark.sql.functions._ //for udf()
    //val toInt    = udf[Int, String]( _.toInt)
    //maxTemps = dfCSVTxns2.withColumn("max_temp", toInt(dfCSVTxns2("max_temp"))). //.cast(IntegerType)).
    //  groupBy(dfCSVTxns2("year")).agg(max("max_temp").alias("max_temp"))

    timeEndSec = DateTime.now()
    timeEnd = System.nanoTime()
    println("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec")
    println("Elapsed Time (micro sec): "+ ( timeEnd-timeStart)/1e3 +" micro sec")

    //Using SparkSQL
    print("Read csv ... SQL way into dataframe\n")
    timeStartSec = DateTime.now()
    timeStart = System.nanoTime()
    val dfSQLTxns = spark.sql("SELECT * FROM csv.`/home/kiran/km/km_big_data/data/data_year_max_temp.csv`")
    timeEndSec = DateTime.now()
    timeEnd = System.nanoTime()
    print("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec\n")
    println("Elapsed Time (micro sec): "+ ( timeEnd-timeStart)/1e3 +" micro sec")

  }

  def getElapsedSeconds(start: DateTime, end: DateTime): Int = {
    val elapsed_time = new Duration(start.getMillis(), end.getMillis())
    val elapsed_seconds = elapsed_time.toStandardSeconds().getSeconds()
    (elapsed_seconds)
  }

}
