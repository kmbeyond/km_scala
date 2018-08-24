package sparkstreaming

//package com.allstate.bigdatacoe.vision.stats

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.databricks.spark.avro._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.reflect.ClassTag

/**
  * Activities performed by this job:
  *   1. Consume Fact Span events from Kafka topic, in batch mode
  *     a. Perform offset management (so that we will consume from where we left off in the earlier batch)
  *     b. Read from topic and write (append to existing data) as a single file in avro format onto HDFS
  *   2. Generate stats
  *     a. group by end point id and compute mean and standard deviation
  *     b. include timestamp also
  *   3. Publish stats
  *     a. Publish the stats to the Stats Kafka topic, with endpoint_id as key
  */

object GenerateFactSpanStats {
  val logger: Logger = Logger.getLogger(this.getClass.getName)
  val currentTime = new Timestamp(System.currentTimeMillis)
  val config = new Properties()

  implicit val formats = Serialization.formats(NoTypeHints)

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  var spanSchema: Schema = _
  var statsSchema: Schema = _
  var valueSerializer: KafkaAvroSerializer = _
  var valueDeserializer: KafkaAvroDeserializer = _

  def main(args: Array[String]): Unit = {
    var configFile = sys.env("VISION_HOME") + "/conf/vision.conf"

    args.sliding(2, 2).toList.collect {
      case Array("--configFile", argConfigFile: String) => configFile = argConfigFile
      case _ => logger.error("Unknown arguments.")
        usage()
        System.exit(100)
    }

    logger.info("********** Using config file " + configFile + "  ********")
    if (!new File(configFile).exists) {
      logger.error("Configuration file " + configFile + " does not exist. Either set the environment variable VISION_HOME or pass the --configFile parameter.")
      usage()
      System.exit(100)
    }
    config.load(Source.fromURL("file://" + configFile).bufferedReader())

    val conf = new SparkConf()
      .setMaster("yarn")
      .setAppName("Generate Vision span stats")

    val spark = SparkSession.builder
      .config(conf)
      .enableHiveSupport()
      .getOrCreate

    consumeFactSpanEvents()
    val stats = generateStats(spark)
    //stats.show(1000, false)
    publishStats(spark, stats)
  }

  def consumeFactSpanEvents(): Unit = {
    val bootstrapServers = config.getProperty("vision.kafka.bootstrapServers")
    val factSpanTopic = config.getProperty("vision.kafka.factSpanTopic")
    val schemaRegistryUrl = config.getProperty("vision.schemaRegistry.url")

    val offsets = getOffsets
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._

    logger.info("********** Consuming Vision Span Events......  ********")
    val spanEvents = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("subscribe", factSpanTopic)
      .option("startingOffsets", offsets)
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load
      .mapPartitions(currentPartition => {
        if (spanSchema == null) {
          val restService = new RestService(schemaRegistryUrl)
          val valueSchema = restService.getLatestVersion(factSpanTopic + "-value")
          val parser = new Schema.Parser
          spanSchema = parser.parse(valueSchema.getSchema)
          logger.debug("***** Span Schema is :\n " + spanSchema.toString(true))
        }
        if (valueDeserializer == null) {
          val kafkaProps = Map("schema.registry.url" -> schemaRegistryUrl)
          val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 20)
          valueDeserializer = new KafkaAvroDeserializer(client)
          valueDeserializer.configure(kafkaProps, false)
          logger.debug("********** Value Deserializer created  ********")
        }

        currentPartition.map(rec => {
          val topic = rec.getAs[String]("topic")
          val offset = rec.getAs[Long]("offset")
          val partition = rec.getAs[Int]("partition")
          var event: String = ""
          try {
            logger.debug("Topic is : " + topic + " offset is : " + offset + " partition is : " + partition + " value is : " + rec.getAs[Array[Byte]]("value").toString)
            event = valueDeserializer.deserialize(rec.getAs[String]("topic"), rec.getAs[Array[Byte]]("value"), spanSchema).asInstanceOf[GenericRecord].toString
          } catch {
            case e: Exception =>
              logger.error("Error occurred while deserializing the message at partition: " + partition + ", offset: " + offset)
              logger.error(e.getMessage)
          }
          (topic, partition, offset, event)
        })
      }).toDF("topic", "partition", "offset", "event")
      .cache()

    val eventJson = spark.sqlContext.read.json(spanEvents
      .filter($"event" =!= "")
      .map(row => row.getAs[String]("event")))

    val spanHistory = config.getProperty("vision.factSpan.rawDataLocation")

    if (eventJson.head(1).length > 0) {
      logger.debug("Writing span events to HDFS ... ")
      //TODO: Should we do intelligent coalescing???
      spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

      eventJson.select($"endpoint_id".cast(IntegerType).alias("endpoint_id"),
        $"application_id".cast(LongType).alias("application_id"),
        $"host_id".cast(IntegerType).alias("host_id"),
        $"domain_id".cast(IntegerType).alias("domain_id"),
        $"method".cast(StringType).alias("method"),
        $"duration".cast(IntegerType).alias("duration"),
        $"status_code".cast(IntegerType).alias("status_code"),
        $"error_occurred".cast(BooleanType).alias("error_occurred"),
        ($"span_created_at".cast(LongType)/1000).cast(TimestampType).alias("span_created_at")
      ).withColumn("row_create_ts", current_timestamp) //Will tell us when the record got entered into Hadoop!
       .withColumn("year", year($"span_created_at"))
       .withColumn("month", month($"span_created_at")) //lpad(month($"span_created_at"), 2, "0")) => 08
       .coalesce(1)
       .write.partitionBy("year", "month")
       .mode("append")
       .avro(spanHistory)


     // If there is no new message on a partition, then no message from that partition will be consumed.
     // This will cause that partition to be skipped while writing the new offset file.
     // So use the partition details from the current offsets file while creating new offsets.
     implicit val formats = org.json4s.DefaultFormats
     val oldOffsets = parse(offsets).extract[Map[String, Any]]
       .get(factSpanTopic)
       .getOrElse(Map("0" -> -999))
       .asInstanceOf[Map[String, BigInt]]
       .toSeq.toDF("partitionString", "offsetBigInt")
       .withColumn("topic", lit(factSpanTopic))
       .select($"topic",
         $"partitionString".cast(IntegerType).alias("partition"),
         $"offsetBigInt".cast(LongType).alias("offset"))

     val newOffsets = spanEvents.select($"topic", $"partition", $"offset")
       .union(oldOffsets)
       .groupBy($"topic", $"partition")
       .max("offset")
       .select($"topic", $"partition", $"max(offset)" as "offset")
       .flatMap(rec => Map(rec.getAs[String]("topic") -> Map(rec.getAs[Int]("partition") -> (rec.getAs[Long]("offset") + 1))))
       .collect

     storeOffsets(newOffsets)
    } else {
      logger.info("No new events present on the topic after the provided offsets.")
      System.exit(101)
    }
  }


  def getOffsets: String = {
    val offsetsDir = config.getProperty("vision.kafka.factSpanOffsetsDir")
    val offsetsFile = config.getProperty("vision.kafka.factSpanOffsetsFile")
    val factspanOffsetFile = offsetsDir + "/" + offsetsFile

    val offsets = Source.fromFile(factspanOffsetFile).getLines.mkString
    logger.info("Got offsets from file : " + offsets)
    offsets
  }

  def storeOffsets(offsets: Array[(String, Map[Int, Long])]): Unit = {
    val offsetsDir = config.getProperty("vision.kafka.factSpanOffsetsDir")
    val offsetsFile = config.getProperty("vision.kafka.factSpanOffsetsFile")
    val factSpanOffsetFile = offsetsDir + "/" + offsetsFile

    val result = new HashMap[String, HashMap[Int, Long]]()
    offsets.foreach({ tp =>
      val parts = result.getOrElse(tp._1, new HashMap[Int, Long])
      parts.putAll(tp._2)
      result += tp._1 -> parts
    })

    val newOffsets = Serialization.write(result)
    logger.info("New offsets : " + newOffsets)

    val currentTimestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val renameFile = new File(factSpanOffsetFile).renameTo(new File(factSpanOffsetFile + "_" + currentTimestamp))
    if (renameFile)
      logger.info("Renamed " + factSpanOffsetFile + " to " + factSpanOffsetFile + "_" + currentTimestamp)
    else
      logger.info("Renaming current offset file failed! Overwriting it.")

    val pw = new PrintWriter(new FileOutputStream(factSpanOffsetFile, false))
    pw.write(newOffsets)
    pw.close()
    logger.info("Writing offsets completed.")
  }

  def generateStats(spark: SparkSession): DataFrame = {
    val spanHistory = config.getProperty("vision.factSpan.rawDataLocation")
    import spark.sqlContext.implicits._
    val spanData = spark.read.avro(spanHistory)

    logger.debug("Generating the stats from the historical data....")
    import org.apache.spark.sql.functions._
    //import spark.sqlContext.implicits._
    val iWinIntSeconds = config.getProperty("vision.kafka.factSpanWindowInterval")
    val errorThresholds = spanData
      .withColumn("errors", when($"error_occurred" === true, 1).otherwise(0))
      .groupBy($"endpoint_id", window((col("span_created_at").cast(LongType)/1000).cast(TimestampType), iWinIntSeconds+" seconds"))
      .agg( avg("errors").multiply(100).alias("err_pct") )
      .groupBy($"endpoint_id")
      .agg( avg($"err_pct").as("err_mean"), stddev_pop($"err_pct").as("err_stddev") )
      //.filter($"err_stddev" =!= 0)

    val appThresholds = spanData
      .filter($"error_occurred" === false)
      .groupBy($"endpoint_id")
      .agg(avg($"duration").as("mean"), stddev_pop($"duration").as("stddev"))
      .filter($"stddev" =!= 0)
      .join(errorThresholds, "endpoint_id")

    //appThresholds.show(false)
    appThresholds
  }



  def publishStats(spark: SparkSession, stats: DataFrame): Unit = {
    val bootstrapServers = config.getProperty("vision.kafka.bootstrapServers")
    val statsTopic = config.getProperty("vision.kafka.statsTopic")
    val schemaRegistryUrl = config.getProperty("vision.schemaRegistry.url")

    import spark.sqlContext.implicits._

    val kafkaMsgs = stats.mapPartitions(currentPartition => {
      if (statsSchema == null) {
        val restService = new RestService(schemaRegistryUrl)
        val valueSchema = restService.getLatestVersion(statsTopic + "-value")
        val parser = new Schema.Parser
        statsSchema = parser.parse(valueSchema.getSchema)
      }
      if (valueSerializer == null) {
        val kafkaProps = Map("schema.registry.url" -> schemaRegistryUrl)
        val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 20)
        valueSerializer = new KafkaAvroSerializer(client)
        valueSerializer.configure(kafkaProps, false)
      }

      var avroRecord: GenericRecord = null
      currentPartition.map(rec => {
        avroRecord = new GenericData.Record(statsSchema)

        val endpoint_id = rec.getAs[Int]("endpoint_id")
        avroRecord.put("endpoint_id", endpoint_id)

        val statsDur = new GenericData.Record(statsSchema.getField("duration").schema())
        statsDur.put("mean", rec.getAs[Double]("mean"))
        statsDur.put("stddev", rec.getAs[Double]("stddev"))
        avroRecord.put("duration", statsDur)

        val statsErr = new GenericData.Record(statsSchema.getField("errors").schema())
        statsErr.put("mean", rec.getAs[Double]("err_mean"))
        statsErr.put("stddev", rec.getAs[Double]("err_stddev"))
        avroRecord.put("errors", statsErr)

        avroRecord.put("create_timestamp", currentTime.getTime)
        val statRecord = valueSerializer.serialize(statsTopic, avroRecord)
        (endpoint_id, statRecord)
      })
    }).toDF("endpoint_id", "statRecord")

    logger.info("Number of records about to publish are : " + kafkaMsgs.count)
    //kafkaMsgs.show(false)
    kafkaMsgs.selectExpr("CAST(endpoint_id AS STRING) AS key", "statRecord AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("topic", statsTopic)
      .save

    logger.info("Completed publishing the stats!")
  }

  def usage(): Unit = {
    logger.error(
      """
        |Usage:
        | spark2-submit --class com.allstate.bigdatacoe.vision.stats.GenerateFactSpanStats \
        |              visionanomalydetection_2.11-<version>.jar \
        |              [--configFile <absolute_path_to_config_file>]
        |
        | configFile:  If not specified, uses the default one, if exists at
        |             the path $VISION_HOME/conf/vision.conf.
        |
        |
      """.stripMargin)
  }
}
