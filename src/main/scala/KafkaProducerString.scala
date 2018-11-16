/**
  * Created by kiran on 2/6/17.
  * Description:
  *   The program gives a report of total sale quantity per a time period & product
  * Input/Source data format: (txn_no,txn_dt,mem_num,store_id,sku_id,qty,paid): 1000000003,2017-06-01 08:00:00,1345671,S876,4685,2,3.98
  * Output/Report: Sends messages in format:
  *      txn_id,txn_date,cust_id,store_id,prod_id,qty,price
  *      1001,2018-10-22,1345671,S3479,3456,2,3.55
  * Steps:
  * 1. Start zookeeper service
  * 2. Start kafka broker(server) at port 9092 ($KAFHA_HOME/config/server.properties)
  * 3. Create Kafka topic "kmtxns" if not existing.
  * 4. Start Kafka producer
  * 5. Run the program: This program sends messages to topic
  * 6. Run the consumer program to read the messages:
  * 7. See the message details (key, offset, message, timestamp etc) from topic
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.avro.Schema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.NoTypeHints
//import org.json4s.jackson.Serialization

//import scala.reflect.ClassTag
//import domain.User
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

object KafkaProducerString {

  val logger = Logger.getLogger(this.getClass().getName())

  val bootstrapServers = "localhost:9092"
  //"localhost:9092" //"srv1:9092,srv2:9092,srv3:9092"
  val kafkaTopic = "kmtxns"
  val groupId = "Kafka_producer_001"

  val props = new java.util.Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  //props.put("security.protocol", "SASL_PLAINTEXT")
  //props.put("sasl.kerberos.service.name", "kafka")
  //props.put("client.id", UUID.randomUUID().toString())


  //implicit val formats = Serialization.formats(NoTypeHints)

  //implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
  //  org.apache.spark.sql.Encoders.kryo[A](ct)


  def main(args: Array[String]): Unit = {

    //logger.log(Level.INFO, "main........")
    //val conf = new SparkConf().setAppName("Spark Producer of Event Stats to Kafka").setMaster("local[10]")
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("INFO")

    //val sTopic = kafkaTopic.split(",").last
    //logger.info("main : kafkaParamsProp = " + props + "   topics= " + sTopic)

    //random data generator
    import scala.util.Random

    val keyList = List("111", "222", "333")
    println("Random key="+ keyList(new Random().nextInt(keyList.size)) )

    val custList = List("8578602", "1563487", "1345671", "1269034")
    val storeList = List("S3479", "S2854", "S876", "S3695", "S398", "S2854")
    val prdList = List("8725", "3456", "4685", "1480", "7889", "1482")
    val qtyList=List(1,2,3,4,5,6)
    val priceList = List(9.99, 2.99, 3.55, 10.00)
    print("Random val="+ storeList(new Random().nextInt(storeList.size)) )

    println("Building producer..")
    val producer = new KafkaProducer[String, String](props)  //Long
    println("Building producer.. done")


    var avroRecord: GenericRecord = null
    for (i <- 1001 to 1003) {
      //for( i <- 9999999 to 9999999) {
      //val key_id = i
      val key_id = keyList(new Random().nextInt(keyList.size))
      val valValue = i+
        ",2018-10-22,"+
        custList(new Random().nextInt(custList.size))+","+
        storeList(new Random().nextInt(storeList.size))+","+
        prdList(new Random().nextInt(prdList.size))+","+
        qtyList(new Random().nextInt(qtyList.size))+","+
        priceList(new Random().nextInt(priceList.size))

      println(getDT()+": sending key="+key_id+"; rec: ("+ valValue +"); ")
      producer.send(new ProducerRecord[String, String](kafkaTopic, key_id.toString(), valValue.toString()))
      println(getDT()+": sent.")
      producer.flush()
      Thread.sleep(10000)
    }

    producer.close()

  }

  def getDT(): String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
  def getType[T](v: T) = v
}
