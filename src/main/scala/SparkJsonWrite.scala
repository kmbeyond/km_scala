import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.when

/* Sample data:
+--------+-------+
|item_cat|item_cd|
+--------+-------+
|C001    |1001   |
|C001    |1004   |
|C002    |1003   |
|C003    |1001   |
|C003    |1004   |
|C004    |1003   |
|C004    |1002   |
|C005    |1002   |
|C006    |1002   |
+--------+-------+

Output:
1)
{"item_cd" : "1001", "item_cat" :["C001", "C003"]}
{"item_cd" : "1002", "item_cat" :["C004", "C005","C006"]}
{"item_cd" : "1003", "item_cat" :["C002", "C004"]}
{"item_cd" : "1004", "item_cat" :["C001", "C003"]}

2)
{"item_cd" : "1001", "item_dtls": {"cat_list":["C001", "C003"], "desc":"aaa"}}
{"item_cd" : "1002", "item_dtls": {"cat_list":["C004", "C005","C006"], "desc":"ccc"}}
{"item_cd" : "1003", "item_dtls": {"cat_list":["C002", "C004"], "desc":"ddd"}}
{"item_cd" : "1004", "item_dtls": {"cat_list":["C001", "C003"], "desc":"fff"}}


*/

object SparkJsonWrite {

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
      .appName("Spark Read JSON Explode")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    //val sJSONPath = "file:///C://km//data_spark_write_json"
    val sJSONPath = "/home/kiran/km/km_big_data/data/data_spark_write_json"


    import spark.implicits._

    val dfCatItems = Seq(("C001", "1001")
                    ,("C001", "1004")
                    ,("C002", "1003")
                    ,("C003", "1001")
                    ,("C003", "1004")
                    ,("C004", "1003")
                    ,("C004", "1002")
                    ,("C005", "1002")
                    ,("C006", "1002")
    ).toDF("item_cat", "item_cd")
    dfCatItems.show(false)

    import org.apache.spark.sql.functions._
    val dfItemCats = dfCatItems.groupBy("item_cd").agg(collect_list("item_cat").as("item_cat"))
      //withColumn("json_struct", struct($"item_cd", $"item_cat_list")).
      //drop("item_cd", "item_cat_list")

    //dfItemCats.coalesce(1).write.json(sJSONPath+"_"+getDT())


    val dfItems = Seq(("1001", "aaa")
      ,("1002", "ccc")
      ,("1003", "ddd")
      ,("1004", "fff")
    ).toDF("item_cd", "desc")

    val dfItemDtls = dfItems.join(dfItemCats, Seq("item_cd") ).
      withColumn("item_dtls", struct($"item_cat".as("cat_list"), $"desc"))

    dfItemDtls.drop("desc", "item_cat").
      coalesce(1).write.json(sJSONPath+"_nested_"+getDT())

  }
  
  def getDT(): String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
}




