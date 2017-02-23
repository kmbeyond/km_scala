import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
/****** WORKING***
  * Created by kiran on 2/14/17.
  */
object SparkRegTempTableQuery {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext TempTable Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")

    val sc = spark.sparkContext
    val sqlContext =  spark.sqlContext

    /* // ***Other way of defining sc & sqlContext ***
    val sparkConf = new SparkConf()
      .setAppName("SQLContext TempTable Demo").setMaster("local")
      .set("spark.executor.memory", "2g")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    */

    //NOT WORKING
    //case class order_items(order_item_id: Int, order_id: Int, cust_id: Int, prod_id: Int, prod_name: String)

    val schema_order_items =
      StructType(
        StructField("order_item_id", IntegerType, false) ::
          StructField("order_id", IntegerType, false) ::
          StructField("cust_id", IntegerType, false) ::
          StructField("prod_id", IntegerType, false) ::
          StructField("prod_name", StringType, false) :: Nil)

    val lines = sc.textFile("/home/kiran/km/km_hadoop/data/data_scala_txns")
      .map(x => x.split("\t"))
      //.map{ x => order_items( x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toString()) } //NOT WORKING
      .map{ x => Row( x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toString) }

    println("lines: " + lines.toString())
    lines.foreach(println)

    import sqlContext.implicits._

    println("Converting to DataFrame...")
    val orderItemsDF = sqlContext.applySchema(lines, schema_order_items) //WORKS
    //val orderItemsDF = sqlContext.createDataFrame(lines, schema_order_items) //WORKS
    //orderItemsDF.printSchema()

    println("Registering temp table...")
    //orderItemsDF.createTempTable("order_items") //NOT WORKING HERE
    //orderItemsDF.createTempView("order_items") //WORKS
    orderItemsDF.createOrReplaceTempView("order_items")

    sqlContext.sql("select * from order_items").show()

  }
}
