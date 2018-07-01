import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by kiran on 2/8/17.
  */
object Spark00readList {

  case class CityTemps(city: String, avgLow: Float, avgHigh: Float)

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("Scala RDD/DF Example from List")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext =  spark.sqlContext
    //import sqlContext._

    println("Using List/parallelize")
    val listRDD = sc.parallelize( Seq(("Chicago", -20.5, 30.5), ("Los Angeles", 25.8, 41.6), ("Denver", 24.8, 31.3) ))
    //.toDF("city", "avgLow", "avgHigh") //Not working in IntelliJ
    val seqColumns = Seq("city", "avgLow", "avgHigh")
    val listDF = sqlContext.createDataFrame(listRDD).toDF(seqColumns: _*)
    listDF.printSchema()
    listDF.show()



  }
}
