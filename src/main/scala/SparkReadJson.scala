import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

/***** WORKING ****
  * Created by kiran on 2/8/17.
  */
object SparkReadJson {

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
val sqlContext = spark.sqlContext

    val lines1 = spark.read.json("/home/kiran/km/km_hadoop/data/data_nested_struct_col.json")
    //Sample: {"id":1,"nested_col": {"key1": "1.1", "key2": ["1.21", "1.22"], "key3": {"key3_1": "1.31", "key3_2": "1.32"}}}
    //nested_col is struct with 3 columns (key1 is string, key2 is Array() and key3 is struct)
    lines1.show(false)
    lines1.printSchema()

    //This withColumn("a.b.c") is considering as a new column, but selects the correct column
    //lines2.withColumn("a.b.c", lines2("a.b.c")).show()

    println("After changing column type...")
    val nestedCol = lines1.withColumn("id", lines1("id").cast(IntegerType)).
      withColumn("nested_col", struct(lines1("nested_col.key1").cast(FloatType).as("key1"),
                                       lines1("nested_col.key2"),
                                        struct(lines1("nested_col.key3.key3_1").cast(FloatType).as("key3_1"),
                                               lines1("nested_col.key3.key3_2"))
                                          .as("key3")))

    nestedCol.printSchema()
    //nestedCol.show(20)

    val lines2 = spark.read.json("/home/kiran/km/km_hadoop/data/data_nested_struct_col2.json")
    //data: {"a": {"b": {"c": "1.31", "d": "1.11"}}, "TimeStamp": "2017-02-18", "id":1}
    lines2.show(false)
    lines2.printSchema()

    /*val df2 = lines2.withColumn("a", struct(
                                        struct(
                                            lines2("a.b.c").cast(DoubleType).as("c"),
                                            lines2("a.b.d").as("d")
                                        ).as("b")))
                .withColumn("TimeStamp", lines2("TimeStamp").cast(DateType))
*/

  //We can't reference child column directly to update **EXPLORE**
    val df2 = lines2.withColumn("'a'.'b'.'c'",
        lines2("'a'.'b'.'c'").cast(DoubleType).as("c")
      )

    df2.printSchema()
    //df2.show(false)


    //df2.createOrReplaceTempView("test")
    //val df3 = sqlContext.sql("describe test")
    //df3.show(false)

    //Flatten structure
    val lines3 = spark.read.json("/home/kiran/km/km_hadoop/data/data_nested_struct_col3.json")
    lines3.printSchema()

    //val df3 = lines3.select(flattenSchema(lines3.schema):_*)
    //val df3 = lines3.withColumn("field2_2", (col("field2")).toString())
      //.drop("field2")
    //df3.printSchema()
    //df3.show()
  }
  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        //case a: ArrayType => {
          //a.productIterator.map(s2 => flattenSchema(s2.asInstanceOf[StructType].in, colName))
          /*
          var itr = a.productIterator
          while(itr.hasNext)
            flattenSchema(itr.next().asInstanceOf[StructType], colName))
*/
//Array(a.productIterator.to[StructField])
        //}
        case _ => Array(col(colName))
      }
    })
  }

}
