import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 2 options:
  * 1) Read directly as JSON
  * 2) Read as text & extract each element
  */
object SparkReadJson {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    //spark.conf.set("spark.executor.memory", "2g")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val sJSONPath = "file:///C://km//avro//sample_avro_data_as_json.json"
    //Sample data:
    //{"performCheck" : "N", "clientTag" :{"key":"111"}, "contactPoint": {"email":"abc@gmail.com", "type":"EML"}}
    //{"performCheck" : "N", "clientTag" :{"key":"222"}, "contactPoint": {"email":"def@gmail.com", "type":"EML"}}
    //{"performCheck" : "Y", "clientTag" :{"key":"333"}, "contactPoint": {"email":"ccc.com", "type":"EML"}}

    import spark.implicits._

//DIRECTLY READ AS JSON
/*    val ds = spark.read.json(sJSONPath)
    ds.show(false)

    val ds2 = ds.withColumn("key", ds.col("clientTag.key"))
      .withColumn("contact_type", ds.col("contactPoint")("type"))
      .withColumn("contact_email", ds.col("contactPoint.email"))
      //.drop("contactPoint")
      //.drop("clientTag")
      .withColumn("contactPoint.valid", when(ds.col("contactPoint.email").contains("@") === true, "Y").otherwise("N"))

    ds2.show(false)
*/


//READ AS TEXT FILE & PROCESS AS JSON
    import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
    import org.apache.spark.sql.functions._                     // include the Spark helper functions


    val fileData = spark.sparkContext.textFile(sJSONPath)
      .toDF("file_data")

    fileData.show(false)

    fileData.select(get_json_object($"file_data", "$.clientTag.key").alias("key"),
                    get_json_object($"file_data", "$.contactPoint.email").alias("email"),
                    get_json_object($"file_data", "$.performCheck").alias("performCheck")
    ).withColumn("contactPoint.valid", when(col("email").contains("@") === true, "Y").otherwise("N"))
      .show(false)
  }
}
