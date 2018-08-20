

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Properties;

public class PublishSpanEventsFromDB {

    static final String schemaRegistryUrl = "http://lxe0961.allstate.com:8081";
    static final String spanTopic = "rtalab.allstate.is.vision.test";
    static final String bootstrapServers = "lxe0961.allstate.com:9092,lxe0962.allstate.com:9092," +
            "lxe0963.allstate.com:9092,lxe0964.allstate.com:9092,lxe0965.allstate.com:9092";

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "10.196.186.224:3306/vision";
    static final String USER = "vision-readonly";
    static final String PASSWORD = "********";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        long i = 0;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(
                    "jdbc:mariadb://" + DB_URL, USER, PASSWORD);
            System.out.println("Connected to database successfully...");

            RestService restService = new RestService(schemaRegistryUrl);
            io.confluent.kafka.schemaregistry.client.rest.entities.Schema valueRestResponseSchema =
                    restService.getLatestVersion(spanTopic + "-value");
            Schema.Parser parser = new Schema.Parser();
            Schema avroschema = parser.parse(valueRestResponseSchema.getSchema());

            System.out.println("Got the Avro schema from Schema Registry!\n" + avroschema.toString(true));

            Properties configProperties = new Properties();
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "VisionSpanDataProducer");
            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
            configProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            configProperties.put("sasl.kerberos.service.name", "kafka");

            Producer producer = new KafkaProducer<String, String>(configProperties);

            GenericRecord avroRecord = null;
            ProducerRecord<String, Object> record = null;

            stmt = conn.createStatement();
            String sql = ("SELECT endpoint_id, application_id, host_id, domain_id, method, duration, status_code, error_occurred, span_created_at "
                    + "FROM fact_span "
                    //+ "limit 10"
                    + ";");
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                i++;
                try {
                    int endpoint_id = rs.getInt("endpoint_id");
                    long application_id = rs.getLong("application_id");
                    int host_id = rs.getInt("host_id");
                    int domain_id = rs.getInt("domain_id");
                    String method = rs.getString("method");
                    int duration = rs.getInt("duration");
                    int status_code = rs.getInt("status_code");
                    boolean error_occurred = rs.getInt("error_occurred") == 1 ? true : false;
                    Timestamp span_created_at = rs.getTimestamp("span_created_at");

                    //System.out.println("Got Record : " + endpoint_id + "," + application_id + "," + host_id + "," +
                    //        domain_id + "," + method + "," + duration + "," + status_code + "," + error_occurred + "," + span_created_at);

                    avroRecord = new GenericData.Record(avroschema);
                    avroRecord.put("endpoint_id", endpoint_id);
                    avroRecord.put("application_id", application_id);
                    avroRecord.put("host_id", host_id);
                    avroRecord.put("domain_id", domain_id);
                    avroRecord.put("method", method);
                    avroRecord.put("duration", duration);
                    avroRecord.put("status_code", status_code);
                    avroRecord.put("error_occurred", error_occurred);
                    avroRecord.put("span_created_at", span_created_at.getTime());

                    record = new ProducerRecord<>(spanTopic, avroRecord);
                    producer.send(record);
                    System.out.println(" Sent an event for end point " + endpoint_id);
                } catch (Exception e) {
                    System.out.println(" Error while sending the event." + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Successfully published " + i + " events!");
            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
