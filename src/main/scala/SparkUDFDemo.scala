import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext,SparkSession}
import org.apache.spark.sql.SQLContext._


/**
  * Created by kiran on 2/9/17.
  */

object SparkUDFDemo {
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
  }
}