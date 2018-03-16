
/**
  * Created by kiran on 2/1/17.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{Map,SynchronizedMap, HashMap}

object SparkWordCountByPartition {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filepath = "/home/kiran/km/km_hadoop/data/data_wordcount"

    //val lines = sc.parallelize( Seq("This is fist line", "This is second line", "This is third line"))
    val lines = sc.textFile(filepath, 1).filter(x => (x.trim() != ""))

    def removeSpecialChars(s: String): String =
    {
      s.toLowerCase replaceAll ("[^a-zA-Z0-9 ]", "")
    }

//-----Method#1: processing inside mapPartitions without a custom function
    lines.
      mapPartitions( prtn => {
        prtn.
          map(removeSpecialChars).
          flatMap(line => line.split(" ")).
          map(x => (x,1))
      }).reduceByKey(_+_).
      //.map(x => (x._1, x._2)) //.groupBy(x => (x._1, x._2))
      takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

//-----Method#1: END

//-----Method#2: Using all processing inside a function
    //function to remove special characters

    def wordsCount(line: Iterator[String]) : Iterator[(String, Int)] = {
      var lineValid = ""
      var lineEach = ""
      var hm = new HashMap[String, Int]
      var wd = ""
      var wdsList = Array("")
      while (line.hasNext) {
        lineEach = line.next()
        lineValid = removeSpecialChars(lineEach)
        wdsList = lineValid.split(" ")
        for ( wd <- wdsList ) {
          if(hm.contains(wd))
            hm(wd) = hm(wd) +1
          else
            hm.put(wd, 1)
        }
      }
      return hm.iterator
    }


    lines.
      mapPartitions(wordsCount).
      reduceByKey(_+_).
      takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
    //Array((what,9), (a,8), (the,8), (has,8), (in,7), (but,5), (with,5), (and,5), (of,5), (it,4))
//-----Method#2: END




//Usual method


  }
}
