import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkNGramMapPartitionsDemo {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    //val data = sc.parallelize(Seq("Hello World, it","is a nice day"))
    val data = sc.textFile("/home/kiran/km/km_hadoop/data/data_customers", 10)

    val trigrams1 = data.flatMap(_.replace(" ", "_").sliding(3))
    trigrams1.collect()

    val trigrams2 = data.mapPartitions(_.flatMap( x => (x.replace(" ","_").sliding(3))))
    //SAME AS:      data.mapPartitions(prtn => prtn.flatMap( x => (x.replace(" ","_").sliding(3))))
    trigrams2.collect()

    val trigrams3 = data.mapPartitionsWithIndex{case (ind, prt) => prt.flatMap(x => x.replace(" ","_").sliding(3)) }
    trigrams3.collect()

  }
}
