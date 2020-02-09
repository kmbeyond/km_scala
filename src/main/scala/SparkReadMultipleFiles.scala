import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkReadMultipleFiles {

  def main(args: Array[String]) {

    //val filePathSrc = "C:\\km\\as_AIA\\test_sample"
    val filePathSrc = "/home/kiran/km/km_big_data/data"

    //val filePathDest = "C:\\km\\as_AIA\\test_sample\\Spark_output_001"
    val filePathDest = "/home/kiran/km/km_big_data/data/fileslist"

    val spark = SparkSession
      .builder()
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("Read Files")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.wholeTextFiles(filePathSrc, 10)

    //This returns contents of each file as one record (same as number of files)
    data.map{ case (filename, cont) => cont}.take(1)


    //list of files & lines in each file
    var files = data.map{ case (filename, cont) => filename+"  [ Rows="+cont.split("\n").size+" ]"}.
      zipWithIndex().
      map(x => (x._2+1, x._1)).
      sortByKey()

    files.collect().foreach(println)

    files.saveAsTextFile(filePathDest+"_"+getDT())

    //Get an RDD of all lines
    val lines = data.flatMap{ case (filename, cont) => cont.split("\n") }
    lines.count()


  }
  def getDT() : String = {
    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    return dateFormatter.format(new Date())
    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
}
