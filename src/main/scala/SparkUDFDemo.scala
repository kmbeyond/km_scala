import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext,SparkSession}
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions.udf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
/**
  * Created by kiran on 2/9/17.
  */

object SparkUDFDemo {

  case class CityTemps(city: String, avgLow: Float, avgHigh: Float)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scala UDF Example")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext =  spark.sqlContext
    //import sqlContext._

    val df = sc.parallelize( List(("Chicago", -20.5, 30.5), ("Los Angeles", 25.8, 41.6) ))
      //.toDF("city", "avgLow", "avgHigh") //Not working in IntelliJ

    //val sqlContext = new SQLContext(sc) //Deprecated

    //Register the temp table
    //df.registerTempTable("citytemps") //NOT WORKING IN IntelliJ
    sqlContext.createDataFrame(df).toDF("city", "avgLow", "avgHigh").createOrReplaceTempView("citytemps")

    // Register the UDF with our SQLContext
    sqlContext.udf.register("CTOF", (degreesCelcius: Double) => ((degreesCelcius * 9.0 / 5.0) + 32.0))

    sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()



    //val dataRDD = spark.read.options(Map(("header" -> "false"), ("delimiter" -> ","))).csv("/home/kiran/km/km_hadoop/data/data_city_temps")
    //val dataDF = dataRDD.toDF("city", "avgLow", "avgHigh")
    //dataDF.printSchema()

    val dataRDD = sc.textFile("/home/kiran/km/km_hadoop/data/data_city_temps")
    val dataRDD2 = dataRDD.map(_.split(',')).map(x => CityTemps(x(0).substring(1, x(0).length-1), x(1).toFloat, x(2).toFloat))
    //val dataDF = dataRDD2.toDF() //NOT WORKING
    val dataDF = sqlContext.createDataFrame(dataRDD2)
    dataDF.printSchema()
    dataDF.show()

    //val df2 = sc.parallelize( List(("Chicago", -20.5, 30.5), ("Los Angeles", 25.8, 41.6) ))

  }
}