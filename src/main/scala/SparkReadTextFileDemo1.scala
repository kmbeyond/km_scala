import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileDemo1 {

  case class CityTemps(city: String, avgLow: Float, avgHigh: Float)

  def main(args: Array[String]) {

    //Sample data as in List()
    val filePathSrc = "C:\\km\\as_AIA\\test_sample"
          //"file:///home/kiran/km/km_hadoop/data/data_city_temps"
          //"hdfs:///user/kiran/data_user.csv"
   
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("Scala UDF Example")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext =  spark.sqlContext
    //import sqlContext._


    val dataRDD = sc.textFile(filePathSrc)

    val rowRDD = dataRDD.filter(_.trim() != "").map(_.split(','))
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10),
                  x(11),x(12),x(13), x(14), x(15), x(16), x(17), x(18)))
      .foreach(println)



/* //City temps data

    println("Using textFile: read as case_class(), createDataFrame()...")
    //val dataRDD = sc.textFile(filepath)
    val dataRDD2 = dataRDD.map(_.split(',')).map(x => CityTemps(x(0).substring(1, x(0).length-1), x(1).toFloat, x(2).toFloat))
    //val dataDF = dataRDD2.toDF() //NOT WORKING
    val dataDF = sqlContext.createDataFrame(dataRDD2)
    dataDF.printSchema()
    dataDF.show()
*/
    import org.apache.spark.sql.functions._

    println("Using csv read................")
    val csvRDD = spark.read.options(Map(("header" -> "true"), ("delimiter" -> ","))).csv(filePathSrc)
    val csvDF = csvRDD.toDF() //"city", "avgLow", "avgHigh")
        .withColumn("Premium2", regexp_replace(col("Premium"), "\\$", ""))
      .withColumn("Premium3", regexp_replace(col("Premium2"), ",", ""))
        //.createOrReplaceTempView("mytempTable")
      //.write.saveAsTable("schemaName.tableName")

    csvDF.printSchema()
    csvDF.show()

    //sqlContext.sql("insert into dru_kmiry. as select * from mytempTable");

  }
}
