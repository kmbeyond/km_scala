
import java.lang.Exception

import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils //KM:Commented due to older version
//import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext.getOrCreate
import org.apache.spark.rdd.WholeTextFileRDD
//import org.apache.spark.sql.hive.HiveContext.implicits._

/**
  * Created by kiran on 2/6/17.
  * This program gets data from stream & writes to a directory based on datetime.
  * Run the merge command to copy all that data to a destination directory:
  * $ hdfs dfs -getmerge /home/kiran/km/km_hadoop_op/op_spark/op_streaming/2017*0/pa*  /home/kiran/km/km_hadoop_op/op_spark/op_streaming/logs_2017-02-17/logs2.txt
  *
  * Changes:
  * KM: Commented due to this runs using older version of Kafka
  */

object SparkStreamKafkaTopicSaveFiles {

  def main(args: Array[String]) {

    val warehouseLocation = "file:/home/kiran/km_hadoop_fs/warehouse"
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Streaming data to Hive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      //.config("spark.driver.allowMultipleContexts", "true")
      //.enableHiveSupport()
      .getOrCreate()

    //set new runtime options
    //spark.conf.set("spark.sql.shuffle.partitions", 6)
    //spark.conf.set("spark.executor.memory", "2g")
    //spark.conf.set("hive.exec.dynamic.partition.mode", "non-strict")
    //spark.conf.getAll.mkString("\n").foreach(print)


    //val ssc = new StreamingContext("local[2]", "Streaming from Kafka topic", Seconds(5))
    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    //val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092,localhost:9093")

    //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val kafkaParams: Map[String, String] = Map(
      //"zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092",
      //"metadata.broker.list" -> "localhost:9092,localhost:9093",
      "group.id" -> "test"
      //,"key.deserializer" -> ""

    )
    val inputTopic = "txnstopic"
    //data format: txn_id,cust_id,prod_id,qty,amount

    //parallelism
    //val numPartitionsOfInputTopic = 1
    //val streams = (1 to numPartitionsOfInputTopic) map { _ =>
    //KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER)
    //[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder]
    //KafkaUtils.createStream(ssc, "km-lnv300-lmint:2181", "test", Map(inputTopic -> 1))
    //}
    //streams.foreach(println)

/* //KM:Commented due to older version
    //val strm = KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER)
    val strm = KafkaUtils.createStream(ssc, "km-lnv300-lmint:2181", "test", Map(inputTopic -> 1))
    //print stream data
    //strm.map(x => x._2).print()

    //val strm = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,inputTopic)
    //val unifiedStream = ssc.union(streams)
    //unifiedStream.print

    import org.joda.time._
    import java.util.Calendar
    val dateTime = new DateTime(Calendar.getInstance.getTime)
    val sDirName = dateTime.toDateTime.toString("yyyy_MM_dd_hh_mm_ss")

    //val timestamp: Long = System.currentTimeMillis
    //val sDirName = timestamp.toString()
    print("date: " + sDirName)


    //strm.map(x => x._2).
    //  saveAsTextFiles("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/"+ new DateTime(Calendar.getInstance.getTime).toDateTime.toString("yyyy_MM_dd_hh_mm_ss"))
      //each stream request write creates a new data directory & files to it (part-00000)

/*
    strm.foreachRDD{ rdd =>
      rdd.map(x=>x._2).
        saveAsTextFile("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/STRM_"+ new DateTime(Calendar.getInstance.getTime).toDateTime.toString("yyyy_MM_dd_HH_mm_ss"))
        //This creates & overwrites existing file for new data, so make sure to give a new directory everytime
    }
*/

   /*  //for each RDD partition **NOT WORKING
  strm.foreachRDD { rdd =>
    rdd.foreachPartition{ rddPrt =>
      rddPrt.map(x => x._2).
        saveAsTextFiles("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/"+ new DateTime(Calendar.getInstance.getTime).toDateTime.toString("yyyy_MM_dd_hh_mm_ss"))
    } //each write creates a new data directory & files to it (part-00000)
  }
*/

    /*
    //Real-time analytics
    strm.foreachRDD { rdd =>
      rdd.map(x => x._2.split(",")).
          map(x => (x(1), x(4).toFloat)).
          reduceByKey(_ + _).
          foreach(println)
    }
*/
    ssc.start()

    ssc.awaitTermination()
*/ //KM:Commented due to older version
  }
}