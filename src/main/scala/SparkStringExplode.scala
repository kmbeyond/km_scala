import org.apache.spark.sql.SparkSession

/* Sample data:


Output:
-Get the item counts for each category

*/

object SparkStringExplode {

  def main(args: Array[String]) {

    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .master("local")
      .appName("Spark StringExplode")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dfItems = List(("1001","C001,C003"),("1002","C004,C005,C006"), ("1003","C002,C004"), ("1004", "C001,C003")).
      toDF("item_cd","item_cat")

    val dfCategories = Seq(("C001","Sports"),("C002","Kids"),("C003","Recreation"),("C004","Casual"),("C005","Travel"), ("C006","General")).
      toDF("cat_cd", "cat_descr")

    import org.apache.spark.sql.functions._
    val dfItemCats = dfItems.withColumn("cat_cd2", explode(split($"item_cat", ","))).
      join(dfCategories, $"cat_cd2" === $"cat_cd")
      //drop("item_cat", "cat_cd2")

    dfItemCats.show()

    dfItemCats.
      groupBy("cat_cd").agg(count("*").as("items_count")).
      orderBy("cat_cd").
      show()

  }
}




