/**
  * Created by kiran on 2/2/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.FloatType
import org.apache.spark.{SparkConf, SparkContext}


object SparkRankProductsByPrice {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("KM Spark Session")
      .enableHiveSupport()
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val fileData = spark.sparkContext.textFile("/home/kiran/km/km_big_data/data/products/")

    //------- RDD -----------
    //Check valid records
    val keyMapCounts = fileData.map(x => x.split(","))
      .map(x => (x.size, 1))
      .reduceByKey(_+_)

    val keyMapValid = fileData.map(x => x.split(","))
      .filter(x => x.size==6)
      .map(x => (x(4).toFloat, "["+x(0)+"]  "+x(2)+" @ "+x(4)) )

    keyMapValid.take(10).foreach(println)


    val priceZipped = keyMapValid
      .groupByKey()
      .sortByKey(false)
      .zipWithIndex
      .filter(x => x._2 < 10)   //top 10
      .map(x => (x._2+1, x._1._2))
      .flatMapValues(x=>x)
      //.saveAsTextFile(args(0))

    priceZipped.take(10).foreach(println)


    //---------DataFrame & rank functions
    val prodDF = spark.read.csv("/home/kiran/km/km_big_data/data/products/").
      toDF("prod_id", "prod_category","prod_name", "flg","price","img_url")
    prodDF.printSchema()

    //prodDF.filter($"price">1000).show()

    val prodDF2 = prodDF.withColumn("price_flt", $"price".cast(org.apache.spark.sql.types.FloatType))
      .drop("price")
      .withColumnRenamed("price_flt", "price")
    prodDF2.printSchema()

    //val windowSpec = org.apache.spark.sql.expressions.Window.partitionBy("col1").orderBy("col2")
    val winSpecPrice = org.apache.spark.sql.expressions.Window.orderBy($"price".desc)

    import org.apache.spark.sql.functions._
    prodDF2.withColumn("rank", rank().over(winSpecPrice)).
      withColumn("dense_rank", dense_rank().over(winSpecPrice)).
      withColumn("row_number", row_number().over(winSpecPrice)).
      show()


  }
}


/* -- OUTPUT--
+-------+-------------+--------------------+---+--------------------+-------+----+----------+----------+
|prod_id|prod_category|           prod_name|flg|             img_url|  price|rank|dense_rank|row_number|
+-------+-------------+--------------------+---+--------------------+-------+----+----------+----------+
|    208|           10| SOLE E35 Elliptical|   |http://images.acm...|1999.99|   1|         1|         1|
|    496|           22|  SOLE F85 Treadmill|   |http://images.acm...|1799.99|   2|         2|         2|
|     66|            4|  SOLE F85 Treadmill|   |http://images.acm...|1799.99|   2|         2|         3|
|    199|           10|  SOLE F85 Treadmill|   |http://images.acm...|1799.99|   2|         2|         4|
|   1048|           47|Spalding Beast 60...|   |http://images.acm...|1099.99|   5|         3|         5|
|    488|           22| SOLE E25 Elliptical|   |http://images.acm...| 999.99|   6|         4|         6|
|    694|           32|Callaway Women's ...|   |http://images.acm...| 999.99|   6|         4|         7|
|    695|           32|Callaway Women's ...|   |http://images.acm...| 999.99|   6|         4|         8|
|     60|            4| SOLE E25 Elliptical|   |http://images.acm...| 999.99|   6|         4|         9|
|    197|           10| SOLE E25 Elliptical|   |http://images.acm...| 999.99|   6|         4|        10|
 */
