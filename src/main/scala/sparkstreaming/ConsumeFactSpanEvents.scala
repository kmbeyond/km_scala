package sparkstreaming

//package com.allstate.bigdatacoe.vision.streaming

import java.io.{File, PrintWriter, StringWriter}
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, mean, row_number, when}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.reflect.ClassTag

object ConsumeFactSpanEvents {
  val logger: Logger = Logger.getLogger(this.getClass.getName)
  val config = new Properties()

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  var spanSchema: Schema = _
  var statsSchema: Schema = _
  var alertsSchema: Schema = _
  var valueDeserializer: KafkaAvroDeserializer = _
  var valueSerializer: KafkaAvroSerializer = _

  var broadCastedStatsData: Broadcast[Map[Integer, (Double, Double, Double, Double)]] = _
  var statsRefreshedTimestamp: Long = _
  var producer: KafkaProducer[String, GenericRecord] = _

  def main(args: Array[String]): Unit = {
    var configFile = sys.env("VISION_HOME") + "/conf/vision.conf"

    args.sliding(2, 2).toList.collect {
      case Array("--configFile", argConfigFile: String) => configFile = argConfigFile
      case _ => logger.error("Unknown arguments.")
        usage()
        System.exit(100)
    }

    logger.info(" Using config file " + configFile)
    if (!new File(configFile).exists) {
      logger.error("Configuration file " + configFile + " does not exist. " +
        "Either set the environment variable VISION_HOME or pass the --configFile parameter.")
      usage()
      System.exit(100)
    }
    config.load(Source.fromURL("file://" + configFile).bufferedReader())

    val bootstrapServers = config.getProperty("vision.kafka.bootstrapServers")
    val spanTopic = config.getProperty("vision.kafka.factSpanTopic")
    val schemaRegistryUrl = config.getProperty("vision.schemaRegistry.url")
    val groupId = config.getProperty("vision.kafka.consumerGroupId")
    val windowInterval = config.getProperty("vision.kafka.factSpanWindowInterval").toInt
    val slideInterval = config.getProperty("vision.kafka.factSpanSlideInterval").toInt
    val outOfOrderWindows = config.getProperty("vision.factspan.outOfOrderWindows").toInt
    val alertsTopic = config.getProperty("vision.kafka.alertsTopic")
    val configRefreshInterval = config.getProperty("vision.stats.configRefreshInterval").trim.toLong

    val conf = new SparkConf()
      .setMaster("yarn")
      .setAppName("Consume Vision span events")

    val spark = SparkSession.builder
      .config(conf)
      .enableHiveSupport()
      .getOrCreate

    val kafkaConsumerParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[KafkaAvroDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "schema.registry.url" -> schemaRegistryUrl,
      "sasl.kerberos.service.name" -> "kafka",
      "security.protocol" -> "SASL_PLAINTEXT",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val ssc = new StreamingContext(spark.sparkContext, Seconds(slideInterval))
    logger.info("Window interval is " + windowInterval + ", Slide interval is : " + slideInterval)
    import spark.sqlContext.implicits._

    // Consume latest span events
    val eventsStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](List(spanTopic), kafkaConsumerParams)
    ).map(x => {
      try {
        val rec = x.value.asInstanceOf[GenericRecord]
        (rec.get("endpoint_id").asInstanceOf[Integer],
          rec.get("duration").asInstanceOf[Integer],
          rec.get("error_occurred").asInstanceOf[Boolean],
          rec.get("span_created_at").asInstanceOf[Long])
      } catch {
        case e: Exception =>
          logger.error("Span event is not validated against the schema! " + e.getMessage)
          (new Integer(-1), new Integer(0), false, 0L)
      }
    }).filter(_._4 > (System.currentTimeMillis() - outOfOrderWindows * windowInterval * 1000)) // Filter out 'out of window' events
      .window(Seconds(windowInterval), Seconds(slideInterval))

    eventsStream.foreachRDD((rdd) => {
      if (statsRefreshedTimestamp == 0 ||
        configRefreshInterval < (System.currentTimeMillis() - statsRefreshedTimestamp) / 1000) {
        statsRefreshedTimestamp = System.currentTimeMillis()
        broadcastStatsData(spark)
      }

      if (!rdd.isEmpty) {
        val statsData = broadCastedStatsData.value
        //Compute z-score and aggregate
        val categorizedSpanEvents = rdd
          .map(event => {
            val endpoint = event._1
            val stats = statsData.getOrDefault(endpoint, (99999999.0, 99999999.0,99999999.0, 99999999.0)) //Added Error mean & stddev defaults
            (endpoint, event._2, event._3
              , stats._1.asInstanceOf[Double], stats._2.asInstanceOf[Double], stats._3.asInstanceOf[Double], stats._4.asInstanceOf[Double])
          }).toDF("endpoint_id", "duration", "error_occurred", "dur_hist_mean", "dur_hist_stddev", "err_hist_mean", "err_hist_stddev")
          .filter($"dur_hist_mean" =!= 99999999.0)
          .withColumn("zscore", ($"duration" - $"dur_hist_mean") / $"dur_hist_stddev") // Compute Z-Score
          .withColumn("zscoreUnder_3", when($"zscore" <= -3.0, 1).otherwise(0))
          .withColumn("zscore_2To_3", when($"zscore" <= -2.0 && $"zscore" > -3.0, 1).otherwise(0))
          .withColumn("zscore_1To_2", when($"zscore" <= -1.0 && $"zscore" > -2.0, 1).otherwise(0))
          .withColumn("zscore1To_1", when($"zscore" < 1.0 && $"zscore" > -1.0, 1).otherwise(0))
          .withColumn("zscore1To2", when($"zscore" >= 1.0 && $"zscore" < 2.0, 1).otherwise(0))
          .withColumn("zscore2To3", when($"zscore" >= 2.0 && $"zscore" < 3.0, 1).otherwise(0))
          .withColumn("zscoreAbove3", when($"zscore" >= 3.0, 1).otherwise(0))
          .withColumn("errors", when($"error_occurred" === true, 1).otherwise(0))
          .groupBy($"endpoint_id")
          .agg(sum("zscoreUnder_3"), sum("zscore_2To_3"), sum("zscore_1To_2"), sum("zscore1To_1"), sum("zscore1To2"), sum("zscore2To3"), sum("zscoreAbove3"),
            max("dur_hist_mean").alias("dur_hist_mean"), mean("errors").multiply(100).alias("err_mean"), max("err_hist_mean").as("err_hist_mean"), max("err_hist_stddev").as("err_hist_stddev")
          )
          .withColumn("err_zscore", ($"err_mean" - $"err_hist_mean") / $"err_hist_stddev") // Compute Error Z-Score
          .withColumn("err_zscoreUnder_3", when($"err_zscore" <= -3.0, 1).otherwise(0))
          .withColumn("err_zscore_2To_3", when($"err_zscore" <= -2.0 && $"err_zscore" > -3.0, 1).otherwise(0))
          .withColumn("err_zscore_1To_2", when($"err_zscore" <= -1.0 && $"err_zscore" > -2.0, 1).otherwise(0))
          .withColumn("err_zscore1To_1", when($"err_zscore" < 1.0 && $"err_zscore" > -1.0, 1).otherwise(0))
          .withColumn("err_zscore1To2", when($"err_zscore" >= 1.0 && $"err_zscore" < 2.0, 1).otherwise(0))
          .withColumn("err_zscore2To3", when($"err_zscore" >= 2.0 && $"err_zscore" < 3.0, 1).otherwise(0))
          .withColumn("err_zscoreAbove3", when($"err_zscore" >= 3.0, 1).otherwise(0))


        // Send these events to alert topic
        categorizedSpanEvents.foreachPartition(currentPartition => {
          if (producer == null) {
            val configProperties = new HashMap[String, Object]()
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "VisionAlertsProducer")
            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
            configProperties.put("schema.registry.url", schemaRegistryUrl)
            configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            configProperties.put("sasl.kerberos.service.name", "kafka")

            producer = new KafkaProducer[String, GenericRecord](configProperties)
            logger.debug("Created a producer object for this partition!")
          }
          if (alertsSchema == null) {
            val restService = new RestService(schemaRegistryUrl)
            val valueSchema = restService.getLatestVersion(alertsTopic + "-value")
            val parser = new Schema.Parser
            alertsSchema = parser.parse(valueSchema.getSchema)
          }

          var avroRecord: GenericRecord = null

          currentPartition.foreach(rec => {
            try {
              avroRecord = new GenericData.Record(alertsSchema)
              val endpoint_id = rec.getAs[Integer]("endpoint_id")
              avroRecord.put("endpoint_id", endpoint_id)
              val statsDur = new GenericData.Record(alertsSchema.getField("duration").schema()) //Get duration child from message
              val statsErr = new GenericData.Record(alertsSchema.getField("errors").schema()) //Get Error child

              statsDur.put("mean", rec.getAs[Double]("dur_hist_mean"))
              statsDur.put("zscoreUnder_3", rec.getAs[Int]("sum(zscoreUnder_3)"))
              statsDur.put("zscore_2To_3", rec.getAs[Int]("sum(zscore_2To_3)"))
              statsDur.put("zscore_1To_2", rec.getAs[Int]("sum(zscore_1To_2)"))
              statsDur.put("zscore1To_1", rec.getAs[Int]("sum(zscore1To_1)"))
              statsDur.put("zscore1To2", rec.getAs[Int]("sum(zscore1To2)"))
              statsDur.put("zscore2To3", rec.getAs[Int]("sum(zscore2To3)"))
              statsDur.put("zscoreAbove3", rec.getAs[Int]("sum(zscoreAbove3)"))
              avroRecord.put("duration", statsDur)


              statsErr.put("mean", rec.getAs[Double]("err_hist_mean"))
              statsErr.put("zscoreUnder_3", rec.getAs[Int]("err_zscoreUnder_3"))
              statsErr.put("zscore_2To_3", rec.getAs[Int]("err_zscore_2To_3"))
              statsErr.put("zscore_1To_2", rec.getAs[Int]("err_zscore_1To_2"))
              statsErr.put("zscore1To_1", rec.getAs[Int]("err_zscore1To_1"))
              statsErr.put("zscore1To2", rec.getAs[Int]("err_zscore1To2"))
              statsErr.put("zscore2To3", rec.getAs[Int]("err_zscore2To3"))
              statsErr.put("zscoreAbove3", rec.getAs[Int]("err_zscoreAbove3"))
              avroRecord.put("errors", statsErr)

              avroRecord.put("timestamp", System.currentTimeMillis())

              val message = new ProducerRecord[String, GenericRecord](alertsTopic, endpoint_id.toString, avroRecord)
              producer.send(message)
              logger.debug("Sent an alert event for endpoint_id " + endpoint_id)
              //logger.info("Sent an alert event for endpoint_id " + endpoint_id + " to topic="+alertsTopic+"; msg="+avroRecord.toString())
            } catch {
              case e: Exception =>
                val sw = new StringWriter()
                e.printStackTrace(new PrintWriter(sw))
                logger.error("Error occurred while sending the enriched event : " + rec.mkString(", ") + ". \n" + sw.toString)
            }
          })

          producer.flush()
          // No need to close the producer?
        })
      } else
        logger.info("It seems there are no events flowing in. Received an empty RDD!")
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def broadcastStatsData(spark: SparkSession): Unit = {
    val bootstrapServers = config.getProperty("vision.kafka.bootstrapServers")
    val statsTopic = config.getProperty("vision.kafka.statsTopic")
    val schemaRegistryUrl = config.getProperty("vision.schemaRegistry.url")
    logger.info("About to consume the stats data....")
    try {
      import spark.sqlContext.implicits._
      val statsEvents = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("subscribe", statsTopic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", false)
        .load
        .mapPartitions(currentPartition => {
          if (this.statsSchema == null) {
            val restService = new RestService(schemaRegistryUrl)
            val valueSchema = restService.getLatestVersion(statsTopic + "-value")
            val parser = new Schema.Parser
            this.statsSchema = parser.parse(valueSchema.getSchema)
          }
          if (this.valueDeserializer == null) {
            val kafkaProps = Map("schema.registry.url" -> schemaRegistryUrl)
            val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 20)
            this.valueDeserializer = new KafkaAvroDeserializer(client)
            this.valueDeserializer.configure(kafkaProps, false)
          }

          currentPartition.map(rec => {
            try {
              val event = this.valueDeserializer.deserialize(rec.getAs[String]("topic")
                , rec.getAs[Array[Byte]]("value")
                , this.statsSchema).asInstanceOf[GenericRecord]
              val stats_duration = event.get("duration").asInstanceOf[GenericRecord]
              val stats_errors = event.get("errors").asInstanceOf[GenericRecord]
              (
                event.get("endpoint_id").asInstanceOf[Integer],
                stats_duration.get("mean").asInstanceOf[Double],
                stats_duration.get("stddev").asInstanceOf[Double],
                stats_errors.get("mean").asInstanceOf[Double],
                stats_errors.get("stddev").asInstanceOf[Double],
                event.get("create_timestamp").asInstanceOf[Long])
            } catch {
              case e: Exception =>
                logger.error("Error occurred while deserializng the stats data" + e.getMessage)
                // Create a dummy stats events with endpoint -1 so that we can filter them.
                (new Integer(-1), 0.0, 0.0, 0.0, 0.0, 0L)
            }
          })
        }).toDF("endpoint_id", "dur_mean", "dur_stddev", "err_mean", "err_stddev", "create_timestamp")

      //logger.info("Stats count:" + statsEvents.count())
      val statsData = statsEvents
        .filter($"endpoint_id" > 0)
        .withColumn("rank", row_number().over(Window.partitionBy($"endpoint_id").orderBy($"create_timestamp".desc)))
        .filter($"rank" === 1)
        .map(row => (row.getAs[Integer](0), (row.getAs[Double](1), row.getAs[Double](2), row.getAs[Double](3), row.getAs[Double](4))))
        .collect
        .toMap

      //statsData.foreach(println)
      if (this.broadCastedStatsData != null) {
        this.broadCastedStatsData.unpersist(true)
      }

      this.broadCastedStatsData = spark.sparkContext.broadcast(statsData)
      logger.info("Broadcasted stats data successfully!")
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        logger.error("Error occurred while getting latest stats data and broadcasting it. \n" + sw.toString)
    }
  }

  def usage(): Unit = {
    logger.error(
      """
        |Usage:
        | spark2-submit --class com.allstate.bigdatacoe.vision.streaming.ConsumeFactSpanEvents \
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