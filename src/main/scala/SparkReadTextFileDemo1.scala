import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileDemo1 {

  def main(args: Array[String]) {

    val filepath = "/home/kiran/km/km_hadoop/Projects_praveen_r/Projects/BDHS_Projects/Project for submission/Project 1/Project 1_dataset_bank-full (2).csv"

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Read delimited file")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    //This code works in spark-shell

    val inColumnsFinal = Seq("age", "job", "marital", "education", "default", "balance", "housing", "loan", "contcat", "day", "month",
      "duration", "campaign", "pdays", "previous", "poutcome", "y")


    val dataDF = sc.textFile(filepath).map(x=>x.substring(1,x.size-1).split(';')).
      map(x => ( x(0).toInt, x(1).substring(2,x(1).size-2), x(2).substring(2, x(2).size-2),
        x(3).substring(2, x(3).size-2), x(4).substring(2, x(4).size-2), x(5).toInt,
        x(6).substring(2, x(6).size-2), x(7).substring(2, x(7).size-2), x(8).substring(2, x(8).size-2),
        x(9), x(10).substring(2, x(10).size-2), x(11).toInt,
        x(12).toInt,x(13).toInt,x(14).toInt,
        x(15).substring(2, x(15).size-2), x(16).substring(2, x(16).size-2) )).
      filter(x => x._1 != "age") //.toDF(inColumnsFinal: _*)

    //dataDF.show(20)

    //dataDF.printSchema()


  }
}
