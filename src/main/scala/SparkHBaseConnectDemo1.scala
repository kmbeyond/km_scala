/**
  * Created by kiran on 3/1/17.
  */
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
//import com.cloudera.spark.hbase.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.hbase.KeyValue
import org.apache.spark.rdd.PairRDDFunctions

object SparkHBaseConnectDemo1 {



  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scala UDF Example")
      .getOrCreate

    val sc = spark.sparkContext

    val hBconf = HBaseConfiguration.create()
    hBconf.addResource(new Path("/usr/local/hbase-1.2.4/conf/hbase-site.xml"));
    //System.setProperty("user.name", "hdfs")
    //System.setProperty("HADOOP_USER_NAME", "hdfs")
    //hBconf.set("hbase.master", "localhost:60000")
    hBconf.setInt("timeout", 120000)
    hBconf.set("hbase.zookeeper.quorum", "localhost")
    //hBconf.set("hbase.zookeeper.property.clientPort", "")
    //hBconf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hBconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    // Initialize
    //val admin = new HBaseAdmin(hBconf)
    //if(!admin.isTableAvailable(input_table)) {
    //  val tableDesc = new HTableDescriptor("tbl1")
    //  admin.createTable(tableDesc)
    //}

    //Write to HBase
    hBconf.set(TableInputFormat.INPUT_TABLE, "tbl1")
    val rdd = sc.parallelize(Array(
      Array("Row5", "cf1", "a", "Value5.1"),
      Array("Row5", "cf1", "b", "Value5.1"))

    )


    //val table1Data = rdd.map(InsertStringArray) //.persist(StorageLevel.MEMORY_AND_DISK)

    //new PairRDDFunctions(rdd.map(InsertStringArray)).saveAsHadoopDataset(hBconf)
    sc.stop()


    //Read from HBase table
    hBconf.set(TableInputFormat.INPUT_TABLE, "tbl1")
    hBconf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf1")
    //conf.set(TableInputFormat.SCAN_COLUMNS, "cf1: cf1:a cf1:b cf1:c"); //Specific column families

    var hBaseRDD = sc.newAPIHadoopRDD(hBconf, classOf[TableInputFormat],
                                            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                            classOf[org.apache.hadoop.hbase.client.Result])
    //hBaseRDD.
    println("Number of Records found : " + hBaseRDD.count())

    try {
      hBaseRDD.map(tuple => tuple._2).
                map(result => result.raw())
                .map(f => KeyValueToString(f)).foreach(println)
                //saveAsTextFile(sink)

    } catch {
      case ex: Exception => {
        println(ex.getMessage())
      }
    }



    //new PairRDDFunctions(newrddtohbase.map(convert)).saveAsHadoopDataset(jobConfig)
    sc.stop()


    //sc.stop()

    /*

    val table1RDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).persist(StorageLevel.MEMORY_AND_DISK)



    //-------------//


    val table2Data = loopBacks.map(  {case(rowkey:ImmutableBytesWritable, values:Result) => (Bytes.toString(values.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1"))), values) }).
    persist(StorageLevel.MEMORY_AND_DISK)

    interfaceData.foreach({case(key:String, values:Result) => {println("---> key is " + key)}})

    // Got the table data //

    val joinedRDD = routerData.join(interfaceData).persist(StorageLevel.MEMORY_AND_DISK);
    joinedRDD.foreach({case((key:String, results: (Result, Result))) =>
    {
      println(" key is " + key);
      println(" value is ");
    }
    }
    )

*/


  }

  //Read from HBase
  def KeyValueToString(keyValues: Array[KeyValue]): String = {
    var it = keyValues.iterator
    var res = new StringBuilder
    while (it.hasNext) {
      res.append( Bytes.toString(it.next.getValue()) + ",")
    }
    res.substring(0, res.length-1);
  }

  //Write to HBase
  def InsertInt(a:Int) : Tuple2[ImmutableBytesWritable, Put] = {
    val p = new Put(Bytes.toBytes(a))
    p.add(Bytes.toBytes("columnfamily"), Bytes.toBytes("col_1"), Bytes.toBytes(a))
    new Tuple2[ImmutableBytesWritable,Put](new ImmutableBytesWritable(a.toString.getBytes()), p);
  }
  //Array("Row5", "cf1", "a", "Value5.1")
  //def InsertStringArray(a: Array[String]) : Tuple2[ImmutableBytesWritable, Put] = {
  def InsertStringArray: (Array[String]) => Tuple2[ImmutableBytesWritable, Put] = a => {
    val p = new Put(Bytes.toBytes(a(0)))
    p.add(Bytes.toBytes(a(1)), Bytes.toBytes(a(2)), Bytes.toBytes(a(3)))
    new Tuple2[ImmutableBytesWritable, Put](new ImmutableBytesWritable(a(0).toString.getBytes()), p);
  }
}