
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object SparkReadHiveData {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      //.config("spark.sql.avro.compression.codec", "snappy")
      //.config("spark.io.compression.codec", "snappy") // using lz4 by default
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      //.config("spark.hadoop.fs.defaultFS", "hdfs://192.168.31.14:8020")
      //.config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
      //.config("spark.hadoop.yarn.resourcemanager.address", "192.168.31.14:8032")
      .master("yarn")
      .appName("Spark Hive connection")
      .getOrCreate

    import spark.sqlContext.implicits._
    //import spark.implicits._
    import org.apache.spark.sql.functions._


    val hiveContext = new HiveContext(spark.sparkContext)
    val arDF = hiveContext.sql("select * from dru_sroct.aggregate_report")
    arDF.printSchema()

    arDF.filter($"mode" > 500).show(10)
    arDF.filter($"mode".cast("float") > 300).show(10)
    arDF.filter( $"mode".contains(" ") ).show(10)

    arDF.filter( $"mode".cast("float").isNull).filter($"mode".contains(" ")).show(100)

    arDF.filter($"carrierreferencenumber".contains("033181915599121")).show(10)


  }
}
