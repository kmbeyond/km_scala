import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{SparkSession,SQLContext}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Row
//import org.apache.spark.implicits._

/***** WORKING ****
  * Created by kiran on 2/8/17.
  */
object SparkFileRDDMean {

  def main(args: Array[String]) {

    val filepath = "/home/kiran/km/km_hadoop/Projects_praveen_r/Projects/BDHS_Projects/Project for submission/Project 1/Project 1_dataset_bank-full (2).csv"

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Read delimited file")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    //Option# 1
    var linesRDD = sc.textFile(filepath).map(x=>x.substring(1,x.size-1).split(';')).filter(x => x(0) != "age").
      map(x => ( x(0).toInt, x(1).substring(2,x(1).size-2), x(2).substring(2, x(2).size-2),
        x(3).substring(2, x(3).size-2), x(4).substring(2, x(4).size-2), x(5).toFloat,
        x(6).substring(2, x(6).size-2), x(7).substring(2, x(7).size-2), x(8).substring(2, x(8).size-2),
        x(9).toInt, x(10).substring(2, x(10).size-2), x(11).toInt,
        x(12)toInt,x(13).toInt,x(14).toInt,
        x(15).substring(2, x(15).size-2), x(16).substring(2, x(16).size-2) ))

    linesRDD.take(15).foreach(println)

    //Option#2

    //Get the mean/average of y="yes"
    val dMean = linesRDD.map(x => (1, (if(x._17=="yes") 1 else 0, 1))).
      reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).
      map(x => x._2._1.toDouble/x._2._2).reduce((x,y) => x)

    println("Average:"+dMean)
    //res8: Array[Double] = Array(0.11698480458295547)

    linesRDD=null

  }
}

