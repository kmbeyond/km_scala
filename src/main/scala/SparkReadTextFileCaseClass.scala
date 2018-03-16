import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{SparkSession,SQLContext}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Row
//import org.apache.spark.implicits._

/**
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileCaseClass {

  case class CustSurvey(age: Int, job: String, marital: String, education: String, default0: String, balance: Float, housing: String, loan: String, contcat: String,
                        day: Int, month: String, duration: Int, campaign: Int, pdays: Int, previous: Int, poutcome: String, y: String)


  def main(args: Array[String]) {

    val filepath = "/home/kiran/km/km_hadoop/Projects_praveen_r/Projects/BDHS_Projects/Project for submission/Project 1/Project 1_dataset_bank-full (2).csv"

/*  //Deprecated, use Session builder
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Read delimited file")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
*/
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Read delimited file")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext

    val linesRDD = sc.textFile(filepath).map(x=>x.substring(1,x.size-1).split(';')).
      filter(x => x(0) != "age").//This is to filter out header
      map(x => CustSurvey( x(0).toInt, x(1).substring(2, x(1).size-2), x(2).substring(2, x(2).size-2),
                            x(3).substring(2, x(3).size-2), x(4).substring(2, x(4).size-2), x(5).toFloat,
                            x(6).substring(2, x(6).size-2), x(7).substring(2, x(7).size-2), x(8).substring(2, x(8).size-2),
                            x(9).toInt, x(10).substring(2, x(10).size-2), x(11).toInt,
                            x(12)toInt,x(13).toInt,x(14).toInt,
                            x(15).substring(2, x(15).size-2), x(16).substring(2, x(16).size-2) ))
    //.toDF() //NOT WORKING HERE, BUT worked on Spark commandline

    linesRDD.take(15).foreach(println)


    // Convert to DF
    val sqlContext = spark.sqlContext
    val linesDF = sqlContext.createDataFrame(linesRDD)
    linesDF.printSchema()
    linesDF.show(20)



    //linesDF.map(x => (1, (if (x(16)=="yes") 1 else 0, 1)) ).reduceByKey()
    //SQLContext
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    //linesDF.show(5)

    //linesDF.printSchema()


  }
}
