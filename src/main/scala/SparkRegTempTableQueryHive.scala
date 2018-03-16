import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
/******* WORKING ****
  * Created by kiran on 2/14/17.
  */
object SparkRegTempTableQueryHive {

  def main(args: Array[String]) {

    //Using sparkSession
    val warehouseLocation = "file:/home/kiran/km_hadoop_fs/warehouse"
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Read delimited file")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filepath = "/home/kiran/km/km_hadoop/data/data_scala_txns"
    val fileOptions = Map(("header" -> "false"), ("delimiter" -> "\\t")) //, ("inferSchema", "false") )
    val inColumns = Seq("order_item_id", "order_id", "cust_id", "prod_id", "prod_name")

    val linesDF = spark.read.options(fileOptions)
      .csv(filepath)
      .toDF(inColumns: _*)

    println("Printing data...")
    linesDF.show()

    linesDF.createOrReplaceTempView("order_items")
    spark.sql("select * from order_items").show()
    //spark.catalog.listTables()

    //Hive DB queries
    //spark.sql("create database IF NOT EXISTS kmdb")
    //spark.sql("create table IF NOT EXISTS kmdb.ord_itms (order_item_id Int, order_id Int, cust_id Int, prod_id Int, prod_name String)")
    //spark.sql("insert into kmdb.ord_itms select * from order_items")

    //spark.sql("select * from kmdb.ord_itms").show()
    spark.sql("show databases").show()
    //spark.sql("use kmdb").show()
    spark.sql("show tables").show()
    //spark.sql("INSERT INTO TABLE kmdb.stream_txns SELECT '1,333,3,2,10' ").show()
    spark.sql("SELECT * FROM kmdb.stream_txns").show()

    //Hive partitioning
    //spark.sql("DROP TABLE kmdb.txns_cust")
    //spark.sql("create table IF NOT EXISTS kmdb.txns_cust (txn_id BIGINT, cust_id BIGINT, prod_id BIGINT, qty INT, amount FLOAT) PARTITIONED BY (txn_dt DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //spark.sql("INSERT INTO kmdb.txns_cust PARTITION(txn_dt='2017-02-18') VALUES (1,1,1,5,66)")
    //spark.sql("SET hive.exec.dynamic.partition=true").show()
    //spark.sql("ALTER TABLE kmdb.txns_cust ADD PARTITION(txn_dt='2017-02-18')").show()
    spark.sql("SELECT * FROM kmdb.txns_cust where txn_dt='2017-02-18'").show()
    //spark.sql("ALTER TABLE kmdb.txns_cust DROP PARTITION (txn_dt='2017-02-18')").show()
  }
}
