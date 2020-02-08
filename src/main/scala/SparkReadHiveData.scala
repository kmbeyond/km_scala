
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
/*
  Setup to connect to Hive warehouse
  -Option#1: Copy hive-site.xml to IntelliJ classes path. --WORKED--
     $cp $HIVE_HOME/conf/hive-site.xml /home/kiran/km/km_proj/km_scala/target/scala-2.11/classes/
  -Option#2: Set config parameters in SparkSession --- TRIED BUT NOT WORKING---

 */
object SparkReadHiveData {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      //.config("spark.sql.warehouse.dir", "file:/home/kiran/km_hadoop_fs/warehouse")
      //.config("hive.metastore.warehouse.dir", "file:/home/kiran/km_hadoop_fs/warehouse")
      //.config("spark.executor.memory", "2g")
      //.config("spark.yarn.executor.memoryOverhead", "10g")
      //.config("spark.sql.avro.compression.codec", "snappy")
      //.config("spark.io.compression.codec", "snappy") // using lz4 by default
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      //.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
      //.config("spark.driver.extraClassPath", "/usr/local/apache-hive-2.1.1-bin/")   //driver-class-path
      //.config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
      //.config("spark.hadoop.yarn.resourcemanager.address", "192.168.31.14:8032")
      .master("local")
      .appName("Spark Hive connection")
      .enableHiveSupport()
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.sqlContext.implicits._
    //import spark.implicits._
    import org.apache.spark.sql.functions._
    print(spark.sparkContext.hadoopConfiguration)

    //spark.sql("SHOW DATABASES").show(false)

    //spark.sql("SHOW TABLES IN kmdb").show(false)


    //val hiveContext = new HiveContext(spark.sparkContext)
    //val arDF = hiveContext.sql("select * from dru_sroct.aggregate_report")
    //arDF.printSchema()

    val dfOrders = spark.table("retail_db.orders").select("order_id", "order_date", "order_status")
    dfOrders.show(5)

    val dfOrdItems = spark.table("retail_db.order_items").select("order_item_id", "order_item_order_id","order_item_quantity","order_item_subtotal")
    dfOrdItems.show(5)

    //Get orders that have total more than $1000
    dfOrders.join(dfOrdItems, $"order_id"===$"order_item_order_id" ). //Seq("order_id")
      drop("order_item_order_id").
      groupBy("order_id").agg(sum("order_item_subtotal").as("order_total")).
      filter($"order_total">1000).
      show(5,false)


    //Get top 10 orders --using row_number()
    val winOrdTotal = org.apache.spark.sql.expressions.Window.orderBy($"order_total".desc)

    dfOrders.join(dfOrdItems, $"order_id"===$"order_item_order_id" ). //Seq("order_id")
      drop("order_item_order_id").
      groupBy("order_id").agg(sum("order_item_subtotal").as("order_total")).
      withColumn("rank", row_number().over(winOrdTotal)).
      filter($"rank"<=10).
      show(false)

    dfOrders.join(dfOrdItems, $"order_id"===$"order_item_order_id" ). //Seq("order_id")
      drop("order_item_order_id").
      groupBy("order_id").agg(sum("order_item_subtotal").as("order_total")).
      orderBy(desc("order_total")).
      show(10,false)

    //val arDF = spark.sql("select * from dru_sroct.aggregate_report")
    //arDF.filter($"mode" > 500).show(10)
    //arDF.filter($"mode".cast("float") > 300).show(10)
    //arDF.filter( $"mode".contains(" ") ).show(10)

    //arDF.filter( $"mode".cast("float").isNull).filter($"mode".contains(" ")).show(100)

    //arDF.filter($"carrierreferencenumber".contains("033181915599121")).show(10)


  }
}
