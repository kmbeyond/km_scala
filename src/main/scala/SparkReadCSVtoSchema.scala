
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.{SparkContext,SparkConf}

import org.apache.spark.sql.Row
/****** WORKING***
  * Created by kiran on 2/14/17.
  * Scenario: Read numeric data when it is enclosed by "" & some data values have blank string "". Throws exception: NumberFormatException
  * Input Data format:
  * 2,111,2,2,10
  * 3,"",3,2,10
  */
object SparkReadCSVtoSchema {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val csvPath = //"/home/kiran/km/km_big_data/data/data_txns_cust.csv"
                  "C:\\km\\km_big_data\\data\\data_txns_cust.csv"

//Read directly as csv with header
    print("---Reading csv directly...")
    val fileOptions1 = Map(("header" -> "true"), ("delimiter" -> ",")) //, ("inferSchema", "false") )
    val csvLines = spark.
      read.
      options(fileOptions1).
      csv(csvPath)

    csvLines.printSchema()
    csvLines.show()


//Read with schema
    print("---Reading using schema...")
    import org.apache.spark.sql.types._
    val schema_txns =
      StructType(
        StructField("txn_id2", IntegerType, false) ::
          StructField("cust_id2", StringType, false) ::
          StructField("prod_id2", IntegerType, false) ::
          StructField("qty2", IntegerType, false) ::
          StructField("amt2", FloatType, false) :: Nil)

    val fileOptions = Map(("header" -> "false"), ("delimiter" -> ",")) //, ("inferSchema", "false") )
    val lines = spark.
      read.
      options(fileOptions).
      schema(schema_txns).
      csv(csvPath)

    lines.printSchema()
    lines.show(5)

    var lines2 = lines.withColumn("cust_id", lines("cust_id2").cast("int")) //.printSchema()
    lines2.printSchema()

    var lines3 = lines.withColumn("cust_id", lines("cust_id2").cast(IntegerType)) //.printSchema()
    lines3.printSchema()

  }
}
