import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
  * Created by kiran on 2/20/17.
  */
object SparkDF {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    //val data = sc.textFile("/home/kiran/km/km_hadoop/data/data_customers.csv", 10)
    import spark.implicits._

    //#1: Directly from List of list
    val valList = List( List("item1", 1), List("item2", 0), List("item1", 0), List("item1", 1))
    val dataDF1 = valList.toDF("item", "error")
    dataDF1.show(false)

    //#2: Using RDD & createDataFrame
    val fields = List(
      StructField("item", StringType, true),
      StructField("error", IntegerType, true)
    )

    val schemaStruct = StructType ( List(
      StructField("item", StringType, true),
      StructField("error", IntegerType, true)
    ))

    val dataDF = spark.createDataFrame( spark.sparkContext.makeRDD( List( List("item1", 1), List("item2", 0), List("item1", 0), List("item1", 1)))
                //, StructType(fields)
                )
    dataDF.show()

    import org.apache.spark.sql.functions._
    val dataTotals = dataDF
      .groupBy("item")
      //.sum("error")
      .agg(
        count("*").alias("err_count"),
        sum("error"), //.alias("err_total"),
        mean("error").alias("err_mean"),
        mean("error").multiply(100).alias("err_percentage")
    )

    dataTotals.show()

  }
}
