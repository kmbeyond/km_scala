
/* Usage:
Program accepts 3 arguments:
 1. String: Event topic name
 2. String: Stats topic name
 3. Number: Events/sec
 Syntax:
java -cp /home/kmiry/vision/visionanomalydetection_km_2.11-1.0-tests.jar:/home/kmiry/vision/VisionAnomalyDetection_KM-assembly-1.0.jar:/opt/cloudera/parcels/CDH/jars/json-simple-1.1.1.jar -Djava.security.auth.login.config=/home/kmiry/kerberos/rtalab_vision/rtalab_vision.jaas -Djava.security.krb5.conf=/home/kmiry/kerberos/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false com.allstate.bigdatacoe.vision.ThrottledSpanEventsPublisher rtalab.allstate.is.vision.test10 rtalab.allstate.is.vision.alerts_kiran 100

 */
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ThrottledSpanEventsPublisher {
    private static final int MAX_THREADS = 8;
    private static final int RAMP_UP_TIME = 120;
    private static final String bootstrapServers = "lxe0961.allstate.com:9092,lxe0962.allstate.com:9092,lxe0963.allstate.com:9092"; //,lxe0964.allstate.com:9092,lxe0965.allstate.com:9092";
    private static final String schemaRegistryUrl = "http://lxe0961.allstate.com:8081";
    private static String statsTopic = ""; //Not final, will be replaced with input
    private static String factSpanTopic = ""; //Not final, will be replaced with input
    private static final int latency = 10; // in microseconds
    private static Map<String, StatsEvent> statsData;
    private static List<String> endpoints;
    private static Producer producer;
    private static Schema spanSchema;
    private static final Logger logger = Logger.getLogger(ThrottledSpanEventsPublisher.class);

    public static void main(String[] args) {
        //Input arg[0] for Events topic
        factSpanTopic = args[0].trim();
        if(factSpanTopic.equals("")) {
            System.out.println("MISSING INPUT: Events topic");
            System.exit(1);
        }
        System.out.println("Events topic: " + factSpanTopic);

        //Input arg[1] for Stats topic
        statsTopic = args[1].trim();
        if(statsTopic.equals("")) {
            System.out.println("MISSING INPUT: Stats topic");
            System.exit(1);
        }
        System.out.println("Stats topic: " + statsTopic);

        //Input arg[2] for Events/sec
        int eventsPerSec = Integer.parseInt(args[2].trim());
        if( eventsPerSec <= 0) {
            System.out.println("MISSING/Invalid INPUT: Events/Sec");
            System.exit(1);
        }
        System.out.println("Events/Sec : " + eventsPerSec);

        ThrottledSpanEventsPublisher tsep = new ThrottledSpanEventsPublisher();
        tsep.createProducer();
        KafkaConsumer consumer = tsep.getConsumer();
        spanSchema = tsep.getSchemaFor(factSpanTopic);

        System.out.println("-> Event schema: " + spanSchema);
        System.out.println("-> Stats Schema: " + tsep.getSchemaFor(statsTopic));

        statsData = tsep.getStatsEvents(consumer);
        System.out.println("There are stats for " + statsData.size() + " unique end points.");
        endpoints = new ArrayList<>(statsData.keySet());

        int sleepIntervalInMicros = ((MAX_THREADS * 1000 * 1000) / eventsPerSec) - latency;
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(MAX_THREADS);

        List<Runnable> tasks = new ArrayList<>(MAX_THREADS);

        for (int i = 0; i < MAX_THREADS; i++) {
            Runnable task = () -> {
                try {
                    Map<String, Integer> event = tsep.sendRandomSpanEvents();
                    for (String key : event.keySet()) {
                        logger.debug(Thread.currentThread().getName() + "  Sent event for end point " + key +
                                ", with duration " + event.get(key) + " at " + new Timestamp(System.currentTimeMillis()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            tasks.add(task);
        }
        System.out.println("scheduling task to be executed every " + sleepIntervalInMicros + " micro seconds with an initial delay of 10 seconds.");
        int delay = RAMP_UP_TIME * 1000 / MAX_THREADS;
        for (Runnable task : tasks) {
            scheduledExecutorService.scheduleAtFixedRate(task, 10 * 1000 * 1000, sleepIntervalInMicros, TimeUnit.MICROSECONDS);
            try {
                Thread.sleep(delay);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Schema getSchemaFor(String topicName) {
        Schema schema = null;
        RestService restService = new RestService(schemaRegistryUrl);
        try {
            Schema.Parser parser = new Schema.Parser();
            io.confluent.kafka.schemaregistry.client.rest.entities.Schema valueSchema =
                    restService.getLatestVersion(topicName + "-value");
            schema = parser.parse(valueSchema.getSchema());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schema;
    }

    private void createProducer() {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "spanDataProducer");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        configProperties.put("sasl.kerberos.service.name", "kafka");

        producer = new KafkaProducer<Long, Object>(configProperties);
    }

    private Map<String, Integer> sendRandomSpanEvents() {
        //System.out.println("sendRandomSpanEvents()");
        Map<String, Integer> result = null;
        try {
            GenericRecord avroRecord;
            ProducerRecord<String, Object> record;
            int randomEndpoint = ThreadLocalRandom.current().nextInt(0, endpoints.size());
            String endpoint = endpoints.get(randomEndpoint);

            StatsEvent stats = statsData.get(endpoint);
            avroRecord = new GenericData.Record(spanSchema);
            int lowerBound = Math.max(0, new Double((stats.getMean() - 4 * stats.getStddev())).intValue());
            int upperBound = new Double(stats.getMean()).intValue() + 4 * new Double(stats.getStddev()).intValue();

            int duration = ThreadLocalRandom.current().nextInt(Math.min(lowerBound, upperBound), Math.max(lowerBound, upperBound + 1));
            boolean error_occurred = duration - stats.getMean() < 2 * stats.getStddev();
            long currentTime = System.currentTimeMillis();

            avroRecord.put("endpoint_id", Integer.parseInt(endpoint.trim()));
            avroRecord.put("application_id", 0L);
            avroRecord.put("host_id", 16203034);
            avroRecord.put("domain_id", 283);
            avroRecord.put("method", "GET");
            avroRecord.put("duration", duration);
            avroRecord.put("status_code", error_occurred ? ThreadLocalRandom.current().nextInt(1, 100) : 0);
            avroRecord.put("error_occurred", error_occurred);
            avroRecord.put("span_created_at", currentTime);

            // Sending key may cause data skew on the spark tasks as more events may be published
            // to one partition (due to hash of key).
            //record = new ProducerRecord<>(factSpanTopic, endpointid + "", avroRecord);
            record = new ProducerRecord<>(factSpanTopic, avroRecord);
            producer.send(record);

            result = new HashMap<>(1);
            result.put(endpoint, duration);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private KafkaConsumer getConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "statsConsumer");
        config.put("consumer.timeout.ms", 1000);

        return new KafkaConsumer<String, String>(config);
    }

    private Map<String, StatsEvent> getStatsEvents(KafkaConsumer consumer) {
        Map<String, StatsEvent> stats = new HashMap<>(10000);
        StatsEvent eventStats;
        int retryCount = 10;

        try {
            consumer.subscribe(Collections.singletonList(statsTopic));
            System.out.println("Subscribed to topic " + statsTopic);

            while (true) {
                ConsumerRecords<Long, Object> records = consumer.poll(100);
                if (records.count() < 1)
                    if (retryCount > 0)
                        retryCount--;
                    else
                        break;
                for (ConsumerRecord<Long, Object> record : records) {
                    try {
                        JSONObject currEvent = (JSONObject) (new JSONParser().parse(record.value().toString()));
                        String key = currEvent.get("endpoint_id") + "";

                        StatsEvent currentStats = stats.getOrDefault(key.trim(), null);

                        if (currentStats != null) {
                            long currentStatsTime = currentStats.getCreate_timestamp();
                            long thisStatsTime = (long) currEvent.get("create_timestamp");
                            if (currentStatsTime >= thisStatsTime)
                                continue;
                        }
                        //Get child elements - duration & errors
                        JSONObject objDuration = (JSONObject) currEvent.getOrDefault("duration", null);
                        JSONObject objErrors = (JSONObject) currEvent.getOrDefault("errors", null);

                        //Send message only when duration object is not null
                        if(objDuration != null) {
                            logger.debug("Duration is NOT NULL for endpoint_id="+ key);
                            eventStats = new StatsEvent(new Integer(key).intValue(), (double) objDuration.get("mean"),
                                    (double) objDuration.get("stddev"),
                                    (double) objErrors.get("mean"), (double) objErrors.get("stddev"),
                                    (long) currEvent.get("create_timestamp"));
                            stats.put(key.trim(), eventStats);
                            //System.out.println("Sent event for endpoint_id="+ key);
                        }else { //KN: Not sending if no Duration
                            logger.debug("Duration is NULL for endpoint_id="+ key);
                            //eventStats = new StatsEvent(0, 0.00, 0.00, 0.00, 0.00, 0L);
                        }


                    } catch (Exception e) {
                        System.out.println("Exception for object: "+ record.value().toString()
                                + "\n -->: " + e.toString());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stats;
    }

    public class StatsEvent {
        int endpoint_id;
        double mean, stddev, err_mean, err_stddev;
        long create_timestamp;

        StatsEvent(int endpoint_id, double mean, double stddev, double err_mean, double err_stddev, long create_timestamp) {
            this.endpoint_id = endpoint_id;
            this.mean = mean;
            this.stddev = stddev;
            this.err_mean = err_mean;
            this.err_stddev = err_stddev;
            this.create_timestamp = create_timestamp;
        }

        private int getEndpoint_id() {
            return endpoint_id;
        }

        private double getMean() {
            return mean;
        }

        private double getStddev() {
            return stddev;
        }

        private long getCreate_timestamp() {
            return create_timestamp;
        }
    }
}

