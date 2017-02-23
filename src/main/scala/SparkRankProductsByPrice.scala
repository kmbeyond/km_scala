/**
  * Created by kiran on 2/2/17.
  */

import org.apache.spark.{SparkConf, SparkContext}


object SparkRankProductsByPrice {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Intellij WordCount")
      .set("spark.executor.memory", "2g")
    //.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val fileData = sc.textFile("/home/kiran/km/km_hadoop/data/products/")

    //Check valid records
    val keyMapCounts = fileData.map(x => x.split(","))
      .map(x => (x.size, 1))
      .reduceByKey(_+_)

    val keyMapValid = fileData.map(x => x.split(","))
      .filter(x => x.size==6)
      .map(x => (x(4).toFloat, "["+x(0)+"]  "+x(2)+" @ "+x(4)) )
      .groupByKey()
      .sortByKey(false)
      .zipWithIndex
      .filter(x => x._2 < 10)   //top 10
      .map(x => (x._2+1, x._1._2))
      .flatMapValues(x=>x)

    keyMapValid.foreach(println)
  }
}
