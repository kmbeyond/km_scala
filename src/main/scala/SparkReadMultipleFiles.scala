import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkReadMultipleFiles {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    val data = sc.wholeTextFiles("/home/kiran/km/km_hadoop/data/", 10)

    //This returns contents of each file as one record (same as number of files)
    data.map{ case (filename, cont) => cont}.take(1)


    //list of files & lines in each file
    var files = data.map{ case (filename, cont) => filename+"["+cont.split("\n").size+"]"}.
      zipWithIndex().
      map(x => (x._2+1, x._1)).
      sortByKey()
    files.saveAsTextFile("/home/kiran/km/km_hadoop/data/fileslist")

    //Get an RDD of all lines
    val lines = data.flatMap{ case (filename, cont) => cont.split("\n") }
    lines.count()


  }
}