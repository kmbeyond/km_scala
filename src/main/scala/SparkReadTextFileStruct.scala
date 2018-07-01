import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileStruct {

  case class CityTemps(city: String, avgLow: Float, avgHigh: Float)

  def main(args: Array[String]) {

    //Sample data as in List()
    val filePathSrc = "/home/kiran/km/km_hadoop/data/data_city_temps"

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("Scala File read to StructType Example")
      .getOrCreate

    val sc = spark.sparkContext
    val sqlContext =  spark.sqlContext
    //import sqlContext._


    println("Using textFile: read as Row(), StructType, createDataFrame()...")
    val dataRDD = sc.textFile(filePathSrc)
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



  }
}
