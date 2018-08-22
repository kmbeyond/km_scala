import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, TimestampType, LongType}

object SparkReadAvro {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .config("spark.sql.avro.compression.codec", "snappy")
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

    val fileData = spark.read.format("com.databricks.spark.avro").load(sAvroPath)
      .toDF("endpoint_id", "application_id", "host_id", "domain_id", "method", "duration", "status_code", "error_occurred", "span_created_at", "row_create_ts")
      .withColumn("span_created_at_datetime", ($"span_created_at".cast(LongType)/1000000).cast(TimestampType))
      .withColumn("row_create_ts_datetime", ($"row_create_ts".cast(LongType)/1000).cast(TimestampType))

    fileData.show(false)

  }
}
