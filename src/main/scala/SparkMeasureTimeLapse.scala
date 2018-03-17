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
    val dataRows = sc.textFile("/home/kiran/km/km_big_data/data/data_year_max_temp.csv")
    val maxTemps = dataRows.map(x => x.split(',')).
      filter(x=> (x(0) !="year" && x(2) != "9999" && x(2).matches("[01459]"))).
      map(x => (x(0).toInt, x(1).toInt)).
      reduceByKey((x,y) => (if(x>y) x else y)).
      map(x => (x._1,x._2))

    //maxTemps.foreach(println(_))
    var timeEndSec = DateTime.now()
    var timeEnd = System.nanoTime()
    println("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec")
    println("Elapsed Time (millisec): "+ ( timeEnd-timeStart)/1e9 +" sec")

    //Using DF
    println("Read csv ... into dataframe")
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
    val dfMaxTemps = dfCSVTxns.filter(dfCSVTxns("quality") !== "9999"). // && $"quality" !== "NA") //"&&", "and" NOT WORKING
      groupBy(dfCSVTxns("year")).agg(max("max_temp").alias("max_temp"))

    //import org.apache.spark.sql.functions._ //for udf()
    //val toInt    = udf[Int, String]( _.toInt)
    //maxTemps = dfCSVTxns2.withColumn("max_temp", toInt(dfCSVTxns2("max_temp"))). //.cast(IntegerType)).
    //  groupBy(dfCSVTxns2("year")).agg(max("max_temp").alias("max_temp"))

    timeEndSec = DateTime.now()
    timeEnd = System.nanoTime()
    println("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec")
    println("Elapsed Time (milli sec): "+ ( timeEnd-timeStart)/1e9 +" sec")

    //Using SparkSQL
    println("Read csv ... SQL way into dataframe")
    timeStartSec = DateTime.now()
    timeStart = System.nanoTime()
    val dfSQLTxns = spark.sql("SELECT * FROM csv.`/home/kiran/km/km_big_data/data/data_year_max_temp.csv`")
    timeEndSec = DateTime.now()
    timeEnd = System.nanoTime()
    print("Elapsed time: " + getElapsedSeconds(timeStartSec, timeEndSec) + " sec\n")
    println("Elapsed Time (milli sec): "+ ( timeEnd-timeStart)/1e9 +" sec")

  }

  def getElapsedSeconds(start: DateTime, end: DateTime): Int = {
    val elapsed_time = new Duration(start.getMillis(), end.getMillis())
    val elapsed_seconds = elapsed_time.toStandardSeconds().getSeconds()
    (elapsed_seconds)
  }

}
