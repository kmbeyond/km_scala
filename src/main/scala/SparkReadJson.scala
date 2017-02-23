import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/***** WORKING ****
  * Created by kiran on 2/8/17.
  */
object SparkReadJson {

  def main(args: Array[String]) {

    //Using sparkSession
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Read delimited file")
      //     .config("spark.sql.warehouse.dir", warehouseLocation)
      //     .enableHiveSupport()
      .getOrCreate()
    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")


    val lines1 = spark.read.json("/home/kiran/km/km_hadoop/data/data_nested_struct_col.json")
    lines1.printSchema()
    //lines1.show(5)

    val nestedCol = lines1.withColumn("id", lines1("id").cast(IntegerType)).
      withColumn("nested_col", struct(lines1("nested_col.key1").cast(FloatType).as("key1"),
                                       lines1("nested_col.key2"),
                                        struct(lines1("nested_col.key3.key3_1").cast(FloatType).as("key3_1"),
                                               lines1("nested_col.key3.key3_2"))
                                          .as("key3")))

    nestedCol.printSchema()
    //nestedCol.show(20)

    val lines2 = spark.read.json("/home/kiran/km/km_hadoop/data/data_nested_struct_col2.json")
    lines2.printSchema()

    val df2 = lines2.withColumn("a", struct( struct(lines2("a.b.c").cast(DoubleType).as("c")).as("b")))
                .withColumn("TimeStamp", lines2("TimeStamp").cast(DateType))
    df2.printSchema()
    //df2.show()

    //SQLContext
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //dataDF.take(15).foreach(println)
  }
}
