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


/*
//data

C://km//vision//data//data//sample_events.csv

txn_id,txn_ts,cust_id,item_id,qty,disc_applied
100001,1532547080843,222,25346,2,true
100002,1532547082843,111,76752,1,true
100003,1532547086843,333,54675,3,false
100004,1532547088843,111,46424,1,false
100005,1532547098844,333,13113,1,false
100006,1532547099844,333,23555,3,false
100007,1532547092844,111,56777,2,true
100008,1532547093844,444,25454,1,false
100009,1532547094844,333,34545,2,true
100010,1532547095844,333,23425,2,false
100011,1532547095844,333,25376,2,true
100012,1532547096844,111,64566,1,false
100013,1532547096844,222,54675,3,true
100014,1532547097844,222,46424,1,true
100015,1532547098844,333,13116,1,false
100016,1532547099846,222,23555,3,true
100017,1532547104846,111,23545,2,false
100018,1532547104846,222,23445,1,false
100019,1532547108850,333,56468,2,false
100020,1532547108850,111,64677,2,true

*/
