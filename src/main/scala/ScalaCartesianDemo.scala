import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by kiran on 2/16/17.
  */

object ScalaCartesianDemo {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Cartesian Product Example")
      //.enableHiveSupport()
      .getOrCreate

    val sc = spark.sparkContext

    val cityRDD = sc.parallelize( List(("Chicago", -20.5, 30.5), ("Los Angeles", 25.8, 41.6) )) //.toDF("city", "avgLow", "avgHigh")
    val dayRDD = sc.parallelize(List("Mon","Tue","Wed","Thu","Fri"))

    val carteRDD = cityRDD.cartesian(dayRDD)
    carteRDD.foreach(println)

  }
}