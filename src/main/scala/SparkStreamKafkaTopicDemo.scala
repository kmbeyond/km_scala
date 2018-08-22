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
//import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
//import _root_.kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}
//import org.apache.spark.streaming.kafka.KafkaUtils //*** Uncomment for old version
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._
import org.apache.spark.sql.hive.HiveContext

import org.apache.kafka.common.serialization.StringDeserializer

object SparkStreamKafkaTopicDemo {

  def main(args: Array[String]) {

  //Using SparkSession & SparkContext
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Streaming data from Kafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark.conf.getAll.mkString("\n").foreach(print)

    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    //Above gives Error: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true

    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    ssc.checkpoint("_checkpointing")
    /*//Context using sparkConf *WORKS*
    val sparkConf = new SparkConf()
      .setAppName("Spark Streaming data from Kafka")
      .setMaster("local[*]")
      .set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //This may give error on console that already SparkContext is running & can't create another context, -then
    //Stop sc (for above error): sc.stop()
    */

    val kafkaParams: Map[String, String] = Map(
      //"zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092"   //org.apache.spark.SparkException: Must specify metadata.broker.list or bootstrap.servers
      //"metadata.broker.list" -> "localhost:9092",
      ,"group.id" -> "kafkaStreamData"
      //,"key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      //,"value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      //,"key.deserializer.class" -> "kafka.serializer.StringEncoder"
      ,"zookeeper.session.timeout.ms" ->	"500"
      ,"zookeeper.sync.time.ms" -> "250"
      ,"auto.commit.interval.ms" ->	"1000"
      ,"auto.offset.reset" -> "largest" //"smallest"/"largest": smallest pulls all messages from beginning
      ,"spark.streaming.kafka.maxRatePerPartition" -> ""
      //,"enable.auto.commit" -> false
    )
    val topicsSet: Set[String] = "txnstopic".split(",").map(_.trim).toSet
    //val topicsArray = "txnstopic".split(",")

/*
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    //Total rows & data in this stream
    messages.count().print()
    messages.print()

    //process each RDD
    messages.foreachRDD( rdd =>  {
      val prodSalesTotal = rdd.map(_._2)
        .filter(rec => rec.split(",").length == 7)
        //filter lines if qty>0
        .filter(rec => rec.split(",")(5).toInt > 0)
        //map to curr_datetime, prod_id & qty
        .map(rec => (rec.split(",")(1)+","+rec.split(",")(4), rec.split(",")(5).toInt) )
        .reduceByKey(_ + _)
        //.map(x => System.currentTimeMillis().toString()+","+x._1+","+x._2)
        .map(x => x._1+","+x._2)

      //print("Partitions count: "+x.partitions.length)
      println("Total rows count: " + rdd.count())
      rdd.take(15).foreach(println)
      println("Result rows count:" + prodSalesTotal.count())
      prodSalesTotal.take(15).foreach(println)

      /*
      // Below function call will save the data into HDFS
      if(prodSalesTotal.count()>0)
        //prodSalesTotal.coalesce(1).saveAsTextFile("/home/kiran/km_hadoop_fs/warehouse/prod_qty_" + getDT4FileName)
        prodSalesTotal.toDF().write.mode(SaveMode.Append).save("/home/kiran/km_hadoop_fs/warehouse/prod_qty_" + getDT4FileName)
      */
    })
*/
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
