import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, TimestampType}

/**
  * Created by kiran on 2/20/17.
  */
object SparkDFWindow {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("DF Window")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //val data = sc.textFile("/home/kiran/km/km_hadoop/data/data_customers", 10)

    val dataDF = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("file:///C://km//vision//data//data//sample_events.csv")

    //dataDF.printSchema()
    //dataDF.show()

    //import org.apache.spark.sql.functions.window
    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._

    val dataDF2 = dataDF
      .withColumn("txn_ts_DT", (col("txn_ts").cast("long")/1000).cast(TimestampType))
      .withColumn("disc", when($"disc_applied".cast("boolean") === true, 1).otherwise(0)) //KM: Added for Error score

    dataDF2.show(false)

    //Averages

    //#1: Average by whole dataset
    dataDF2
      .groupBy($"cust_id")
      .agg(
        count("*").alias("txns_count")
        //,sum("qty") //.alias("qty_total"),
        //,avg("qty").multiply(100).alias("qty_avg")
        ,sum("disc").alias("disc_total")
        ,avg("disc").multiply(100).alias("disc_avg")
      )
    .show(false)

    //Average by cust_id & time window (instead of getting avg in whole dataset)
    val dataAggTime = dataDF2
      .groupBy($"cust_id", window($"txn_ts_DT", "120 seconds"))
      .agg(
        count("*").alias("txns_count")
        //,sum("qty").alias("qty_total")
        //,avg("qty").multiply(100).alias("qty_avg")
        ,sum("disc").alias("disc_total")
        ,avg("disc").multiply(100).alias("disc_avg")
    )
    dataAggTime.show(false)

    //Average by cust_id
    val dataAgg = dataAggTime
      .groupBy($"cust_id")
      .agg(
        sum("txns_count").alias("txns_count")
        //,avg("qty_avg")
        ,avg("disc_avg")
      )

    dataAgg.show(false)

  }
}
