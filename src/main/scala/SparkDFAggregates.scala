import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkDFAggregates {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("DF Aggregates")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    //val data = sc.textFile("/home/kiran/km/km_hadoop/data/data_customers", 10)

    import spark.implicits._
    val dataSeq = Seq(
      ("item1", 1), ("item2", 0),("item1", 0), ("item1", 1)
    )

    val dataDF = dataSeq.toDF("item", "error")

    dataDF.show()

    import org.apache.spark.sql.functions._
    val dataTotals = dataDF
      .groupBy("item")
      //.sum("error")
      .agg(
        count("*").alias("err_count"),
        sum("error"), //.alias("err_total"),
        mean("error").alias("err_mean"),
        mean("error").multiply(100).alias("err_percentage")
    )

    dataTotals.show()

  }
}
