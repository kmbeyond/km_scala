import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkDSfromList {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    //spark.conf.set("spark.executor.memory", "2g")

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    import spark.implicits._

    //#1: Directly from List of list
    //case class ItemsList (item: String, count: Int)
    val valList = List( ("item1", 1), ("item2", 0), ("item1", 0), ("item1", 1))
    val dataDS1 = valList//.map{x => Row(x:_*)}
      .toDS()

    dataDS1.show(false)


  }
}
