import java.lang.Exception

//import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
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
  */

object StreamKafkaTopicDemoHiveContext {

  def main(args: Array[String]) {




    val ssc = new StreamingContext("local[2]", "Streaming from Kafka topic", Seconds(5))
    //val ssc = new StreamingContext(spark.sparkContext.getConf, Seconds(5))
    //val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    //val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092,localhost:9093")

    //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    def insertTxn(rec: String): Unit = {
      try {
        //Start Spark Session
        val warehouseLocation = "file:/home/kiran/km_hadoop_fs/warehouse"
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("Spark Streaming data to Hive")
          .config("spark.sql.warehouse.dir", warehouseLocation)
          //.config("spark.driver.allowMultipleContexts", "true")
          .enableHiveSupport()
          .getOrCreate()

        //set new runtime options
        //spark.conf.set("spark.sql.shuffle.partitions", 6)
        //spark.conf.set("spark.executor.memory", "2g")
        //spark.conf.set("hive.exec.dynamic.partition.mode", "non-strict")
        //spark.conf.getAll.mkString("\n").foreach(print)

        print("\nQuery:-->   INSERT INTO kmdb.stream_txns VALUES ('" + rec + "')\n")
        spark.sql("INSERT INTO kmdb.stream_txns VALUES ('" + rec + "')").show()
        //spark.sqlContext.sql("INSERT INTO TABLE kmdb.stream_txns SELECT '" + rec + "' ").show()
        //spark.sql("SELECT * FROM kmdb.stream_txns").show()
      } catch {
        case ex: Exception => {
          print("Exception occured..." + ex.printStackTrace()+"\n")
        }
        case ex: java.io.IOException => {
          println("IOException occured...")
        }
      }
    }

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

    //val str = KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER)
    //val str = KafkaUtils.createStream(ssc, "km-lnv300-lmint:2181", "test", Map(inputTopic -> 1))

    //val str = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,inputTopic)
    //val unifiedStream = ssc.union(streams)
    //unifiedStream.print

    //print stream input
    //str.map(x => x._2).print()

    //Save stream message
    //str.saveAsHadoopFiles("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/" + format.toString() );
    //ssc.textFileStream("/home/kiran/km/km_hadoop_op/op_spark/op_streaming/")


    //spark.sql("show databases").show()
    //spark.sql("create database IF NOT EXISTS kmdb")
    //spark.sql("drop table kmdb.stream_txns")
    //spark.sql("CREATE TABLE IF NOT EXISTS kmdb.stream_txns (v String)") //txn_id Int, cust_id Int, prod_id Int, qty Int, amount Float)")
    //spark.sql("desc kmdb.stream_txns").show()
/*
    str.foreachRDD { rdd =>
        rdd.foreachPartition{ partitionOfRecords =>
          partitionOfRecords.foreach( rec => insertTxn(rec._2) )
        }
      }*/

    //str.map (rec => insertTxn(rec._2) ).print()

    /*
    str.foreachRDD { rdd =>
      rdd.map( rec => insertTxn(rec._2) )
      }
*/


    //ssc.start()

    //ssc.awaitTermination()
  }
}
