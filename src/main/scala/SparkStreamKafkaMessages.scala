/**
  * Created by kiran on 2/6/17.
  * Description:
  *   The program reads messages from Kafka
  * Input/Source data format: Send a message in any format
  * Output/Report: 
  * +------+---------+------+----+-------------+-----------------------+
  * |topic |partition|offset|key |value        |timestamp_2            |
  * +------+---------+------+----+-------------+-----------------------+
  * |kmtxns|3        |0     |null|hello message|2016-11-15 19:18:03.455|
  * +------+---------+------+----+-------------+-----------------------+
  * Steps:
  * 1. Start zookeeper service
  * 2. Start kafka broker(server) at port 9092 ($KAFHA_HOME/config/server.properties)
  * 3. Create Kafka topic "kmtxns" if not existing.
  * 4. Start Kafka producer
  * 5. Run the program
  * 6. Send sample message: hello world
  * 7. See the message details (key, offset, message, timestamp etc) from topic
  */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils //*** Uncomment for old version
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamKafkaMessages {

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
    val ssc = new StreamingContext(sparkConf, Seconds(10))
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
      ).map(rec => (rec.key(), (rec.topic(), rec.partition(), rec.timestamp(), rec.offset(), rec.key(), rec.value().toString())) )
        //.map(_._2)

    //Total rows & data in this stream
    //println(getDT4FileName()+": Messages in this batch:")
    //messages.print()
    //messages.count().print()

    //process each RDD
    messages.foreachRDD( rdd =>  {
      if (!rdd.isEmpty) {


        val prodSalesTotal = rdd.map(_._2)
          .map(rec => (rec._1, rec._2, rec._3, rec._4, rec._5, rec._6))
          .toDF("topic", "partition","timestamp","offset", "key","value")
            .withColumn("timestamp_2", ($"timestamp".cast("long")/1000).cast(TimestampType))
            .drop("timestamp")

        //print("Partitions count: "+x.partitions.length)
        println("-------" + getDT4FileName() + "-------")
        println("Total input messages: " + rdd.count())
        //rdd.take(15).foreach(println)
        println("Result rows count:" + prodSalesTotal.count())
        prodSalesTotal.show(false)

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
