package sparkstreaming

//import com.utils.VisionUtils.{VisionEvent}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.{FloatType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.ClassTag


object SparkStructStreamKafkaConsumer {
  val logger = Logger.getLogger(this.getClass().getName())

  var statsSchema: Schema = _
  var valueDeserializer: KafkaAvroDeserializer = _

  def main(args: Array[String]) {

    logger.info("Class = " + this.getClass().getName())
    logger.info(getDT() + ": SparkStreamKafkaTopic start.")

    val bootstrapServers = "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092"
    //"localhost:9092" //"lxe0730.allstate.com:9092,lxe0731.allstate.com:9092,lxe0731.allstate.com:9092"

    val kafkaTopicName = //"rtalab.allstate.is.vision.ingest"
      "rtalab.allstate.is.vision.stats"
    //"rtalab.allstate.is.vision.alerts"
    //"rtalab.allstate.is.vision.test10" //events
    //"rtalab.allstate.is.vision.alerts_kiran" //stats
    //"rtalab.allstate.is.vision.alerts_durga" //alerts

    val grpId_strm = "KM_TOPIC_Messages_0003"
    val schemaRegistryUrl = "http://lxe0961.allstate.com:8081"

    //Using SparkSession & SparkContext
    /*  // *IMP* Replaced this because of lz4 NoMethodFound error
        val spark = SparkSession
          .builder()
          .master("local[1]")
          .appName("Spark Streaming data from Kafka")
          .getOrCreate()

        spark.conf.set("spark.io.compression.codec", "snappy")
    */
    val conf = new SparkConf()
      .setAppName("Spark Struct Streaming data from Kafka")
      .setMaster("yarn")
      .set("spark.io.compression.codec", "snappy") //I think default using lz4

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    spark.conf.getAll.mkString("\n").foreach(print)
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")


    try {
      import spark.sqlContext.implicits._
      val statsData = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("subscribe", kafkaTopicName)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load
        .mapPartitions(currentPartition => {
          println("*** mapPartitions: Getting Schema ***")
          val valueSchema = new RestService(schemaRegistryUrl).getLatestVersion(kafkaTopicName + "-value")
          println("*** Schema: " + valueSchema.getSchema)
          statsSchema = new Schema.Parser().parse(valueSchema.getSchema)
          println("*** schema parsed: " + statsSchema)
          //statsSchema = parser.parse("{\"type\":\"record\",\"name\":\"visionStats\",\"fields\":[{\"name\":\"endpoint_id\",\"type\":\"long\"},{\"name\":\"mean\",\"type\":\"double\",\"default\":0.0},{\"name\":\"stddev\",\"type\":\"double\",\"default\":0.0},{\"name\":\"createTimestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"default\":0}]}")

          println("*** valueDeserializer ***")
          valueDeserializer = new KafkaAvroDeserializer( new CachedSchemaRegistryClient(schemaRegistryUrl, 20) )
          valueDeserializer.configure(Map("schema.registry.url" -> schemaRegistryUrl), false)

          //println("*** Map.. ***")
          currentPartition.map(rec => {

            val topic = rec.getAs[String]("topic")
            val offset = rec.getAs[Long]("offset")
            val partition = rec.getAs[Int]("partition")

            var event: String = ""
            //event = valueDeserializer.deserialize(

            try{
              //event =  valueDeserializer.deserialize(topic, rec.getAs[Array[Byte]]("value"), statsSchema).asInstanceOf[GenericRecord].toString
              //For Not to deserialize
              event = rec.getAs[Array[Byte]]("value").toString()
            } catch {
              case e: Exception =>
                logger.error("Error occurred while deserializing the message at partition: " + partition + ", offset: " + offset)
                logger.error(e.getMessage)
            }
            (topic, partition, offset, event)

          })
        }).toDF("topic", "partition", "offset", "value")

      //println("*** DF cache & Rec count= "+statsData.count()) //1813
      //statsData.show(2000, false)

      if(statsData.head(1).length <= 0) {
        logger.error("*** Alert! Missing stats/reference data. ***")
        println("*** Alert! Missing stats/reference data. ***")
      }else {

        import org.apache.spark.sql.functions._
        val maxOffsets = statsData
          .groupBy($"partition")
          .agg(max($"offset")
            //,min($"offset")
          )
        println("*** Partitions count= " + maxOffsets.count() + " ***")
        maxOffsets.show(20, false)

        println("*** Topic messages printed.")
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        //logger.error("Error occurred while getting latest stats data and broadcasting it. \n" + sw.toString)
        println("Error occurred while getting data from topic. \n" + sw.toString)
    }

  }

  @throws(classOf[Exception])
  def getRESTSchemaBySubject(schemaURL: String, subjectName: String) : org.apache.avro.Schema = {

    Try(new RestService(schemaURL).getLatestVersion(subjectName).getSchema) match {
      case Success(s) => {
        logger.info(s"Found schema for $subjectName")
        new Schema.Parser().parse(s)
      }
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        null
      }
    }

  }


  def getDT()
  : String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }

  def getType[T](v: T) = v

}



/*

Secure Kafka setting in IntelliJ:
VM options:
-Djava.security.auth.login.config=C:\km\kerberos\rtalab_vision\rtalab_vision.jaas -Djava.security.krb5.conf=C:\km\kerberos\krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false


spark2-submit \
    --keytab /home/kmiry/kmiry.keytab \
    --principal ${HDFS_PRINCIPAL} \
    --jars /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10/spark-streaming-kafka-0-10_2.11-2.3.0.cloudera2.jar,/opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10/kafka-clients-0.10.0-kafka-2.1.0.jar \
    --driver-memory 4g \
    --executor-memory 4g \
    --driver-java-options "-Dlog4j.configuration=${LOG4J_CONF} -Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=${LOG4J_CONF} -Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --class com.allstate.bigdatacoe.vision.streaming.SparkStructStreamKafkaConsumer \
    --conf spark.ui.port=6669 \
    --conf spark.yarn.maxAppAttempts=4 \
    --conf spark.yarn.max.executor.failures=100 \
    --conf spark.task.maxFailures=8 \
    --conf "spark.dynamicAllocation.executorIdleTimeout=30s" \
    --conf "spark.dynamicAllocation.minExecutors=10" \
    --conf spark.dynamicAllocation.initialExecutors=10 \
    /home/kmiry/vision/VisionAnomalyDetection-assembly-1.1_struct.jar >> ${LOG_DIR}/km_test_${processDate}.log 2>&1 &
    
    

*/
