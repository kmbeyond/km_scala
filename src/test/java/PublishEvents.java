
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Add the following VM options to run this class
 * <p>
 * -Djava.security.auth.login.config=/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/rtalab_vision_jaas.conf
 * -Djava.security.krb5.conf=/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/krb5.conf
 * -Djavax.security.auth.useSubjectCredsOnly=false
 */

public class PublishEvents {

    // Variables
    String factSpanTopic = "rtalab.allstate.is.vision.test";
    String statsTopic = "rtalab.allstate.is.vision.stats";
    String bootstrapServers = "lxe0961.allstate.com:9092,lxe0962.allstate.com:9092,lxe0963.allstate.com:9092,lxe0964.allstate.com:9092,lxe0965.allstate.com:9092";
    String schemaRegistryUrl = "http://lxe0961.allstate.com:8081";
    //String factSpanSchemaFile = "/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/VisionFactSpan.avsc";
    String statsSchemaFile = "/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/VisionFactSpan.avsc";
    String appStatsFile = "/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/appThresholds.csv";

    /**
     * Creates a Kafka Producer that can be used to send messages to Kafka topic.
     *
     * @return
     */
    public Producer getProducer() {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "VisionSpanDataProducer");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        configProperties.put("sasl.kerberos.service.name", "kafka");

        Producer producer = new KafkaProducer<Long, Object>(configProperties);

        return producer;
    }

    /**
     * Get the schema for the given topic from schema registry
     */
    public Schema getSchemaFor(String topic) {
        Schema schema = null;
        RestService restService = new RestService(schemaRegistryUrl);
        try {
            io.confluent.kafka.schemaregistry.client.rest.entities.Schema valueSchema =
                    restService.getLatestVersion(topic + "-value");
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(valueSchema.getSchema());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schema;
    }

    /**
     * Creates sample fact span events and sends them onto the fact span events topic as an AVRO message.
     * Currently, only endpoint_id and duration are being sent to the topic.
     * <p>
     * We can add as many fields to the message and then send to topic. Note that schema is backward
     * compatible - so adding new elements is supported any time. But we CAN NOT delete an element from
     * the schema. So being conservative and added only bare minimum fields for this testing.
     * <p>
     * The loop variable can be parameterized if required - but leaving it as we are testing interactively
     * from IDE itself.
     *
     * @param producer
     */
    public void sendSpanEvents(Producer producer) {
        try {
            GenericRecord avroRecord = null;
            ProducerRecord<String, Object> record = null;
            Schema statsSchema = getSchemaFor(factSpanTopic);
            System.out.println(statsSchema);

            long endpointid = 0L;
            long duration = 0L;
            Random r = new Random();

            for (int i = 0; i < 10000000; i++) {
                avroRecord = new GenericData.Record(statsSchema);
                //endpointid = r.nextInt(9) + 1;
                endpointid = 4766254;
                //duration = 160;
                int randomNum = ThreadLocalRandom.current().nextInt(-120, 150 + 1);
                duration = 120 + randomNum;
                boolean error_occurred = randomNum < 0 ? true : false;
                long currentTime = System.currentTimeMillis();

                avroRecord.put("endpoint_id", endpointid);
                avroRecord.put("application_id", 0L);
                avroRecord.put("host_id", 16203034);
                avroRecord.put("domain_id", 283);
                avroRecord.put("method", "GET");
                avroRecord.put("duration", duration);
                avroRecord.put("status_code", 0);
                avroRecord.put("error_occurred", error_occurred);
                avroRecord.put("span_created_at", currentTime);

                //record = new ProducerRecord<>(factSpanTopic, endpointid + "", avroRecord);
                record = new ProducerRecord<>(factSpanTopic, avroRecord);
                producer.send(record);
                System.out.println("  Event # " + i + ": Sent an event for end point " + endpointid +
                        ", with duration " + duration + " at " + new Timestamp(currentTime));

                // TODO: Remove the below line. Used for testing only!
                //Thread.sleep(15000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the csv file that contains the end point statistics and publishes them onto Kafka topic
     * as AVRO messages.
     *
     * @param producer
     */
    public void sendStatsEvents(Producer producer) {
        try {
            Schema schema = new Schema.Parser().parse(new File(statsSchemaFile));
            GenericRecord avroRecord = null;
            ProducerRecord<String, Object> record = null;

            long endpointid = 0L;
            long currentTime = System.currentTimeMillis();

            Scanner scan = new Scanner(new File(appStatsFile));
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                String[] fields = line.split(",");

                avroRecord = new GenericData.Record(schema);
                endpointid = Long.parseLong(fields[0]);
                avroRecord.put("endpoint_id", endpointid);
                avroRecord.put("mean", Double.parseDouble(fields[1]));
                avroRecord.put("stddev", Double.parseDouble(fields[2]));
                avroRecord.put("createTimestamp", currentTime);

                record = new ProducerRecord<>(statsTopic, avroRecord);
                producer.send(record);
                System.out.println("  Sent the stats for endpoint " + endpointid);

                // TODO: Remove the below line. Used for testing only!
                Thread.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method. Gets the Kafka Producer and sends events to Stats topic and Fact Span topic.
     *
     * @param args
     */
    public static void main(String[] args) {
        PublishEvents factspan = new PublishEvents();
        Producer producer = factspan.getProducer();
        // Stats topic has already some messages. So no need to publish new messages.
        //factspan.sendStatsEvents(producer);
        factspan.sendSpanEvents(producer);
        producer.close();
    }
}