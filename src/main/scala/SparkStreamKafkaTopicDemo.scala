/**
  * Created by kiran on 2/6/17.
  */

import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, FloatType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext.getOrCreate
import org.apache.spark.rdd.WholeTextFileRDD
//import org.apache.spark.sql.hive.HiveContext.implicits._

object SparkStreamKafkaTopicDemo {

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Streaming data from Kafka")
      .getOrCreate()

    //spark.conf.getAll.mkString("\n").foreach(print)


    //val ssc = new StreamingContext("local[2]", "Streaming from Kafka topic", Seconds(5))
    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))



    val kafkaParams: Map[String, String] = Map(
      "zookeeper.connect" -> "localhost:2181",
      //"bootstrap.servers" -> "localhost:9092",
      //"metadata.broker.list" -> "localhost:9092",
      "group.id" -> "test"
      //,"key.deserializer.class" -> "kafka.serializer.StringEncoder"
      ,"zookeeper.session.timeout.ms" ->	"500"
      ,"zookeeper.sync.time.ms" -> "250"
      ,"auto.commit.interval.ms" ->	"1000"

    )
    val inputTopic = "txnstopic"
    //data format: txn_id,cust_id,prod_id,qty,amount

    //parallelism
    /*val numPartitionsOfInputTopic = 1
    val streams = (1 to numPartitionsOfInputTopic) map { _ =>
      KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER)
      //[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder]

    }
   streams.foreach(println)
   */
    val strm = KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER)
    //var strm = KafkaUtils.createStream(ssc, "km-lnv300-lmint:2181", "test", Map(inputTopic -> 1))
    strm.map(x => x._2).print()


    //strm.saveAsHadoopFiles("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/" + format.toString() );

    //ssc.textFileStream("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/")

    //val str = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,inputTopic)
    //val unifiedStream = ssc.union(streams)
    //unifiedStream.print


    ssc.start()
    ssc.awaitTermination()
  }
}