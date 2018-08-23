import org.apache.spark.sql.{Row, SparkSession}
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
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    //spark.conf.set("spark.executor.memory", "2g")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //val data = sc.textFile("/home/kiran/km/km_hadoop/data/data_customers.csv", 10)
    import spark.implicits._

    //#1: Directly from List
    val valList = List( ("item1", 1), ("item2", 0), ("item1", 0), ("item1", 1))
    val dataDF1 = valList//.map{x => Row(x:_*)}
      .toDF("item", "error")
    dataDF1.show(false)

    //Make RDD
    val rddSC = spark.sparkContext.parallelize(valList)
    rddSC.foreach(println)

    rddSC.toDF("item", "count").show()

    val rdd = spark.sparkContext.makeRDD( valList )
    rdd.foreach(println)

    //#2: Using RDD & createDataFrame
    val fieldsList = List(
      StructField("item", StringType, true),
      StructField("error", IntegerType, true)
    )

    val schemaStruct = StructType (  fieldsList  )


    val dataDF = spark.createDataFrame( rdd.toJavaRDD()
                //, StructType(fieldsList) //WHY NOT WORKING
                //, schemaStruct
                )
    //dataDF.show()

    val dataDF2 = dataDF.toDF(Seq("item", "error"): _*)

    import org.apache.spark.sql.functions._
    val dataTotals = dataDF2
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

