package flipkart.dsp.camel;

/**
 * Created by bhushan.sk on 31/08/15.
 */
public class ComponentTestConstants {

    public static final String BROKERS_STR = "brokers";
    public static final String BROKERS = "localhost:9092";

    public static final String BOOTSTRAP_SERVERS_STR ="bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static final String BATCH_SIZE_STR = "batchSize";
    public static final int BATCH_SIZE = 50;

    public static final String LINGER_MS_STR = "lingerMs";
    public static final int LINGER_MS = 400;

    public static final String KEY_SERIALIZER_STR = "keySerializer";
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String VALUE_SERIALIZER_STR = "valueSerializer";
    public static final String VALUE_SERIALIZER_KAFKA_STR = "value.serializer";

    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String ACKS_STR = "acks";
    public static final String ACKS = "all";

    public static final String TOPIC_STR = "topic";
    public static final String TOPIC = "camel-kafka-test";
    public static final String TOPIC_ANOTHER = "camel-kafka-foo";

    public static final int RETRY_BACKOFF = 100;
    public static final String RETRY_BACKOFF_STR = "retryBackoffMs" ;

    public static final String PARTITION_KEY_VALUE = "fooPartitionKey";

    public static final String URI = "kafkaAsync:" + BROKERS;

    public static final String KAFKA_TO_URL = "kafkaAsync:" + BROKERS + "?topic=" + TOPIC + "&" + BATCH_SIZE_STR+"=" + BATCH_SIZE+
            "&" + ACKS_STR+"=" + ACKS + "&" + LINGER_MS_STR+"="+ LINGER_MS+"&"+
            "keySerializer=" + KEY_SERIALIZER + "&valueSerializer="+ VALUE_SERIALIZER +"&" +
            RETRY_BACKOFF_STR+"=" + RETRY_BACKOFF;

}
