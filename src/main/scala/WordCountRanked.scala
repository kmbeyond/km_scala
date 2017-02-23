/**
  * Created by kiran on 2/2/17.
  * This has limitation that data is sorted by each reducer
  */

import org.apache.spark.{SparkContext, SparkConf}


object WordCountRanked {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Intellij WordCount")
      .set("spark.executor.memory", "2g")
    //.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val lines = sc.parallelize( List("cat", "elephant", "rat", "rat", "cat","Giraffee","Tiger","Lion","cat","zebra","cat") )
    val wordLengths = lines.map(x => (x.size, x))
    val counts = wordLengths.

      //val lines = sc.textFile("/home/kiran/km/km_hadoop/data/data_wordcount").flatMap(x => x.split(" "))

      //val counts = lines.map(x => (x,1)).reduceByKey(_+_).map(x => (x._2.toInt, x._1+"["+x._2+" times]")).
      groupByKey().
      sortByKey(false).
      zipWithIndex.
      map(x => (x._2+1, x._1._2)).
      flatMapValues(x => x)
      //.filter(x => x._1 <= 4)
      .sortByKey()

    counts.foreach(println)
  }
}

