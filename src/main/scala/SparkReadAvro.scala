import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, TimestampType, LongType, DateType}

/*

pscp C:\Users\kmiry\.ivy2\cache\com.databricks\spark-avro_2.11\jars\spark-avro_2.11-4.0.0.jar kmiry@10.195.247.1:/home/kmiry/jars

$ spark2-shell --jars /home/kmiry/jars/spark-avro_2.11-4.0.0.jar,/home/kmiry/jars/snappy-java-1.1.2.1.jar --conf spark.userClassspathFirst=true --conf spark.executor.extraClassPath="/home/kmiry/jars/snappy-java-1.1.2.1.jar" --conf "spark.io.compression.codec=snappy"

scala> val eventsAvroDF = spark.read.format("com.databricks.spark.avro").load("hdfs:///user/kmiry/vision/raw")
scala> eventsAvroDF.show(10, false)

*/

object SparkReadAvro {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      //.config("spark.sql.avro.compression.codec", "snappy")
      //.config("spark.io.compression.codec", "snappy") // using lz4 by default
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    //spark.conf.set("spark.executor.memory", "2g")

    //val sc = spark.sparkContext
    //sc.setLogLevel("ERROR")

    val sAvroPath = //"hdfs:///user/kmiry/vision/raw"
                    "C:\\km\\avro\\part-00000-f2ea6eb6-eae7-42a5-8a8a-c9ce28271c77-c000.avro"

    import spark.implicits._
    import org.apache.spark.sql.functions._                     // include the Spark helper functions

    //val sqlContext = spark.sqlContext.setConf("spark.sql.avro.compression.codec", "snappy") //uncompressed, snappy, deflate
    //val fileData = sqlContext.read.avro(sAvroPath)

    //import spark.sqlContext.implicits._
    //import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.rank

    val fileData = spark.read.format("com.databricks.spark.avro").load(sAvroPath)

    val groupedDF = fileData.
        toDF("endpoint_id", "application_id", "host_id", "domain_id", "method", "duration", "status_code", "error_occurred", "span_created_at", "row_create_ts").
        withColumn("span_created_at_datetime", ($"span_created_at".cast(LongType)/1000000).cast(TimestampType)).drop("span_created_at").
        withColumn("row_create_ts_datetime", ($"row_create_ts".cast(LongType)/1000).cast(TimestampType)).drop("row_create_ts").
        withColumn("age_in_days", datediff(current_timestamp(), col("span_created_at_datetime"))).filter($"age_in_days" < 15).
        withColumn("created_dt", $"span_created_at_datetime".cast(DateType)).
        groupBy("created_dt", "endpoint_id").agg(count("endpoint_id").alias("endpoints_count"))

    groupedDF.cache()

    //groupedDF.printSchema
    //root
    //|-- created_dt: date (nullable = true)
    //|-- endpoint_id: integer (nullable = true)
    //|-- endpoints_count: long (nullable = false)


//Rank() by created_dt & endpoint_id
    //using Window
    import org.apache.spark.sql.expressions.Window
    val w_desc = Window.partitionBy($"created_dt").orderBy(desc("endpoints_count"))
    val w_asc = Window.partitionBy($"created_dt").orderBy("endpoints_count")

    groupedDF. //filter($"created_dt" === "2018-08-09").
      withColumn("rank", rank().over(w_desc)).
      withColumn("dense_rank", dense_rank().over(w_desc)).
      withColumn("row_number", row_number().over(w_desc)).
      filter($"row_number" <= 10).
      //orderBy($"created_dt", desc("endpoints_count")).
      orderBy($"created_dt", $"row_number").
        show(100, false)


  }
}
