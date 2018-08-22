/**
  * Created by kiran on 2/6/17.
  * Description:
  *   The program gives a report of total sale quantity per a time period & product
  * Input/Source data format: (txn_no,txn_dt,mem_num,store_id,sku_id,qty,paid): 1000000003,2017-06-01 08:00:00,1345671,S876,4685,2,3.98
  * Output/Report: Prints all messages from all partitions
  *   txn_dt, sku_id, qty_total
  *   2017-06-01 08:00:00,3245,2
  *   2017-06-01 08:00:00,4685,7
  *
  *   Messages by each partition.
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
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import _root_.kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._
import org.apache.spark.sql.hive.HiveContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, CanCommitOffsets, OffsetRange}

object SparkStreamKafka010TopicDemo {

  def main(args: Array[String]) {

    //Using SparkSession & SparkContext
    val spark = SparkSession
      .builder()
      .master("local[20]")
      .appName("Spark Streaming data from Kafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //spark.conf.getAll.mkString("\n").foreach(print)

    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    //Above gives Error: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    //StreamingContext without SparkContext
    //val ssc = new StreamingContext("local[2]", "Spark Streaming data from Kafka", Seconds(30))

    //ssc.checkpoint("_checkpointing")

    val kafkaParams: Map[String, Object] = Map(
      //"zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092"   //org.apache.spark.SparkException: Must specify metadata.broker.list or bootstrap.servers
      //"metadata.broker.list" -> "localhost:9092",
      ,"group.id" -> "kafkaStreamDataxx3"
      ,"key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer]
      ,"zookeeper.session.timeout.ms" ->	"500"
      ,"zookeeper.sync.time.ms" -> "250"
      ,"auto.commit.interval.ms" ->	"1000"
      ,"auto.offset.reset" -> "earliest" //latest, earliest, none
      ,"spark.streaming.kafka.maxRatePerPartition" -> ""
      ,"enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("kafkatopic2") //This topic has PartitionCount:10

    //process each RDD
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(
      rdd =>  {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        for (o <- offsetRanges) {
          println(s"Topic=${o.topic}; Partition=${o.partition}; Offset (${o.fromOffset} - ${o.untilOffset})")
        }

        rdd.foreach(println)


      val prodSalesTotal = rdd.map(rec => (rec.key, rec.value)).map(_._2)
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
      //rdd.take(15).foreach(println)
      println("Result rows count:" + prodSalesTotal.count())
      prodSalesTotal.take(50).foreach(println)

        //see messages count by partition
       rdd.map(rec => (rec.partition, 1)).reduceByKey(_ + _).foreach(println)
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
