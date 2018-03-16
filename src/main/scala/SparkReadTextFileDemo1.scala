import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileDemo1 {

  case class CityTemps(city: String, avgLow: Float, avgHigh: Float)

  def main(args: Array[String]) {

    val filepath = "/home/kiran/km/km_hadoop/data/data_city_temps"
    //Sample data as in List()

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scala UDF Example")
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


    println("Using textFile: read as Row(), StructType, createDataFrame()...")
    val dataRDD = sc.textFile(filepath)
    val rowRDD = dataRDD.map(_.split(',')).map(x => Row(x(0).substring(1, x(0).length-1), x(1).toFloat, x(2).toFloat))
    val schema_cityTemp =
      StructType(
        StructField("city", StringType, false) ::
          StructField("avgLow", FloatType, false) ::
          StructField("avgLow", FloatType, false) :: Nil)
    val rowDF = sqlContext.createDataFrame(rowRDD, schema_cityTemp)
    rowDF.printSchema()
    rowDF.show()


    println("Using textFile: read as case_class(), createDataFrame()...")
    //val dataRDD = sc.textFile(filepath)
    val dataRDD2 = dataRDD.map(_.split(',')).map(x => CityTemps(x(0).substring(1, x(0).length-1), x(1).toFloat, x(2).toFloat))
    //val dataDF = dataRDD2.toDF() //NOT WORKING
    val dataDF = sqlContext.createDataFrame(dataRDD2)
    dataDF.printSchema()
    dataDF.show()


    println("Using csv read................")
    val csvRDD = spark.read.options(Map(("header" -> "false"), ("delimiter" -> ","))).csv(filepath)
    val csvDF = csvRDD.toDF("city", "avgLow", "avgHigh")
    csvDF.printSchema()
    csvDF.show()



  }
}
