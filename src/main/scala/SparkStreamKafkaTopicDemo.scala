/**
  * Created by kiran on 2/6/17.
  * Description:
  *   The program gives a report of total sale quantity per a time period & product
  * Input/Source data format: (txn_no,txn_dt,mem_num,store_id,sku_id,qty,paid): 1000000003,2017-06-01 08:00:00,1345671,S876,4685,2,3.98
  * Output/Report: txn_dt, sku_id, qty_total
  *   2017-06-01 08:00:00,3245,2
  *   2017-06-01 08:00:00,4685,7
  *
  * Steps:
  * 1. Start zookeeper service
  * 2. Start kafka broker(server) at port 9092 ($KAFHA_HOME/config/server.properties)
  * 3. Create Kafka topic "txnstopic" if not existing.
  * 4. Start Kafka producer
  * 5. Run the program
  * 6. Send sample message
  * 7. See the files on hive warehouse: prod_by_qty
  */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}
//import org.apache.spark.streaming.kafka.KafkaUtils //*** Uncomment for old version
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._
import org.apache.spark.sql.hive.HiveContext

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions._

object SparkStreamKafkaTopicDemo {

  val sTopicNamesList="kmtxns"
  val refreshInt = 5

  def main(args: Array[String]) {

  //Using SparkSession & SparkContext
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Streaming data from Kafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark.conf.getAll.mkString("\n").foreach(print)
    import spark.sqlContext.implicits._
    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    //Above gives Error: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true

    val ssc = new StreamingContext(spark.sparkContext, Seconds(refreshInt))

    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    //ssc.checkpoint("_checkpointing")
    /*//Context using sparkConf *WORKS*
    val sparkConf = new SparkConf()
      .setAppName("Spark Streaming data from Kafka")
      .setMaster("local[*]")
      .set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(sparkConf, Seconds
    (10))
    //This may give error on console that already SparkContext is running & can't create another context, -then
    //Stop sc (for above error): sc.stop()
    */

    val kafkaParams: Map[String, Object] = Map(
      //"zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092"   //org.apache.spark.SparkException: Must specify metadata.broker.list or bootstrap.servers
      //"metadata.broker.list" -> "localhost:9092",
      ,"group.id" -> "kafkaStreamData"
      ,"key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"zookeeper.session.timeout.ms" ->	"500"
      ,"zookeeper.sync.time.ms" -> "250"
      ,"auto.commit.interval.ms" ->	"1000"
      ,"auto.offset.reset" -> "earliest" //"earliest"/"latest"
      //,"spark.streaming.kafka.maxRatePerPartition" -> ""
      ,"enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicsSet: Set[String] = sTopicNamesList.split(",").map(_.trim).toSet
    //val topicsArray = "txnstopic".split(",")

    val messages =
      KafkaUtils.createDirectStream[String, String](
        //KafkaUtils.createDirectStream[String, String](
        //KafkaUtils.createDirectStream[String, KafkaAvroDeserializer](
        ssc,
        PreferConsistent,
        Subscribe[String, String](sTopicNamesList.split(","), kafkaParams)
      ).map(rec => (rec.key(), rec.value().toString()) )
        .window(Seconds(15), Seconds(refreshInt))
        //.map(_._2)

    //Total rows & data in this stream
    //println(getDT4FileName()+": Messages in this batch:")
    messages.print()
    //messages.count().print()

    //process each RDD
    messages.foreachRDD( rdd =>  {
      if (!rdd.isEmpty) {

        //**** RDD ******
        val prodSalesTotal = rdd.map(_._2)
          .map(rec => rec.split(","))
          .filter(rec => rec.length == 7)
          //filter lines if qty>0
          .filter(rec => rec(5).toInt > 0)
          .filter(rec => rec(0).length() > 3 && rec(1).length() > 9)
          //map to prod_id, qty
          //.map(rec => (rec.split(",")(4), rec.split(",")(5)))
          //.reduceByKey(_ + _).map(x => x._1 + "," + x._2).toDF("prod_id", "qty")
          .map(rec => (rec(0), rec(1), rec(2), rec(3), rec(4), rec(5), rec(6)))
          .toDF("txn_no","txn_dt","mem_num","store_id","sku_id","qty","paid")
          .groupBy($"sku_id")
            .agg(sum("qty").alias("QTY"), bround(sum("paid"),2).alias("REVENUE"))

        //print("Partitions count: "+x.partitions.length)
        println("-------" + getDT4FileName() + "-------")
        println("Total input messages: " + rdd.count())
        //rdd.take(15).foreach(println)
        println("Result rows count:" + prodSalesTotal.count())
        prodSalesTotal.show()

        /*
      // Below function call will save the data into HDFS
      if(prodSalesTotal.count()>0)
        //prodSalesTotal.coalesce(1).saveAsTextFile("/home/kiran/km_hadoop_fs/warehouse/prod_qty_" + getDT4FileName)
        prodSalesTotal.toDF().write.mode(SaveMode.Append).save("/home/kiran/km_hadoop_fs/warehouse/prod_qty_" + getDT4FileName)
      */
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getDT4FileName(): String ={

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)

  }
}
