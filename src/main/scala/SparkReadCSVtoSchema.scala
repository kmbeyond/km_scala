
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
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


    val schema_txns =
      StructType(
        StructField("txn_id", IntegerType, false) ::
          StructField("cust_id", StringType, false) ::
          StructField("prod_id", IntegerType, false) ::
          StructField("qty", IntegerType, false) ::
          StructField("amt", FloatType, false) :: Nil)

    val fileOptions = Map(("header" -> "false"), ("delimiter" -> ",")) //, ("inferSchema", "false") )
    val lines = spark.read.options(fileOptions).
      schema(schema_txns).
      csv("/home/kiran/km/km_hadoop/data/data_txns_cust.csv")

    lines.printSchema()
    lines.show(5)

    lines.withColumn("cust_id", lines("cust_id").cast("int")).printSchema()
    lines.show(5)

  }
}
