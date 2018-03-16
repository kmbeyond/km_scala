/**
  * Created by kiran on 2/1/17.
  */
import org.apache.spark.{SparkContext, SparkConf}


object SparkWordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Intellij WordCount")
      .set("spark.executor.memory", "2g")
    //.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val filepath = "/home/kiran/km/km_hadoop/data/data_wordcount"

    //function to remove special characters
    def removeSpecialChars(s: String): String =
    {
      s.toLowerCase replaceAll ("[^a-zA-Z0-9 ]", "")
    }

    //val lines = sc.parallelize( Seq("This is fist line", "This is second line", "This is third line"))
    val lines = sc.textFile(filepath).filter(x => (x.trim() != ""))

    val counts =
      lines.map(removeSpecialChars).
        flatMap(line => line.split(" ")).
        map(x => (x,1)).
        reduceByKey(_+_).
        //.map(x => (x._1, x._2)) //.groupBy(x => (x._1, x._2))
        takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))


    counts.foreach(println)
  }
}
