import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.functions._

/***** WORKING ****
  * Created by kiran on 2/8/17.
  */
object SparkReadTextFileUDFTransform {

  def main(args: Array[String]) {

    //Using sparkSession
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Read delimited file")
      //     .config("spark.sql.warehouse.dir", warehouseLocation)
      //     .enableHiveSupport()
      .getOrCreate()
    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filepath = "/home/kiran/km/km_hadoop/Projects_praveen_r/Projects/BDHS_Projects/Project for submission/Project 1/Project 1_dataset_bank-full (2).csv"

    val inColumns = Seq("age-job", "marital", "education", "default", "balance", "housing", "loan", "contcat", "day", "month",
      "duration", "campaign", "pdays", "previous", "poutcome", "y")



    val fileOptions = Map(("header" -> "true"), ("delimiter" -> ";")) //, ("inferSchema", "false") )
    val lines = spark.read.options(fileOptions).
      csv(filepath).
      toDF(inColumns: _*)
    //OR USE //spark.read.option("header", "true").option("delimiter",";").csv(filepath).toDF(inColumns: _*).printSchema()  ]

    //lines.show(5)
    //lines.printSchema()


    def trimColumn(str: String): String = {
      str.substring(2, str.size-2)
    }
    def trimColumn_udf = udf(trimColumn _)

    def extrAge(str: String): String = {
      str.substring(1, str.indexOf(';'))
    }
    def extrAge_udf = udf(extrAge _)

    def extrJob(str: String): String = {
      str.substring(str.indexOf(';')+3, str.size-2)
    }
    def extrJob_udf = udf(extrJob _)

    def trimLastColumn(str: String): String = {
      str.substring(2, str.size-3)
    }
    def trimLastColumn_udf = udf(trimLastColumn _)


    val dataDF = lines.withColumn("marital", trimColumn_udf(col("marital")) ).
      withColumn("education", trimColumn_udf(col("education")) ).
      withColumn("default", trimColumn_udf(col("default")) ).
      withColumn("housing", trimColumn_udf(col("housing")) ).
      withColumn("loan", trimColumn_udf(col("loan")) ).
      withColumn("contcat", trimColumn_udf(col("contcat")) ).
      withColumn("month", trimColumn_udf(col("month")) ).
      withColumn("poutcome", trimColumn_udf(lines("poutcome")) ).
      withColumn("y", trimLastColumn_udf(col("y")) ).
      withColumn("job", extrJob_udf(col("age-job")) ).
      withColumn("age-job", extrAge_udf(col("age-job")) ).
      withColumnRenamed("age-job","age")

    dataDF.show(20)
    //dataDF.count
    //dataDF.cache()


    //SQLContext
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    dataDF.take(15).foreach(println)
  }
}
