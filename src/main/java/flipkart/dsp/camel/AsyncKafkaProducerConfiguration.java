package flipkart.dsp.camel;

/**
 * Created by bhushan.sk on 26/08/15.
 * Kafka Producer Configuration
 */


import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

import java.util.Properties;

@UriParams
public class AsyncKafkaProducerConfiguration {

    @UriParam(defaultValue = "100")
    private Integer lingerMs = 100;

    @UriParam
    private String clientId;

    @UriParam(defaultValue = "all")
    private String acks = "all";

    @UriParam(defaultValue = "true")
    private boolean blockOnBufferFull = true;

    @UriParam(defaultValue = "50")
    private Integer batchSize = 50;

    @UriParam
    private String topic;

    @UriParam
    private String keySerializer;

    @UriParam
    private String valueSerializer;

    @UriParam(defaultValue = "5")
    private Integer retries = 5 ;

    @UriParam(defaultValue = "100")
    private Integer reconnectBackoffMs = 100;

    @UriParam(defaultValue = "100")
    private Integer retryBackoffMs = 100;

    public AsyncKafkaProducerConfiguration() {
//        setClientId((this.getTopic() != null ?
//                getTopic() : Thread.currentThread().getName()) +"-producer");
    }

    private static <T> void addPropertyIfNotNull(Properties props, String key, T value) {
        if (value != null) {
            // Kafka expects all properties as String
            props.put(key, value.toString());
        }
    }

    public Properties createProducerProperties() {

        Properties props = new Properties();
        addPropertyIfNotNull(props, "linger.ms", getLingerMs());
        addPropertyIfNotNull(props, "key.serializer", getKeySerializer());
        addPropertyIfNotNull(props, "value.serializer", getValueSerializer());
        addPropertyIfNotNull(props, "acks", getAcks());
        addPropertyIfNotNull(props, "retry.backoff.ms", getRetryBackoffMs());
        addPropertyIfNotNull(props, "client.id", getClientId());
        addPropertyIfNotNull(props, "block.on.buffer.full", getBlockOnBufferFull());
        addPropertyIfNotNull(props, "batch.size", getBatchSize());
        addPropertyIfNotNull(props, "retries", getRetries());
        addPropertyIfNotNull(props, "reconnect.backoff.ms", getReconnectBackoffMs());

        return props;
    }

    public boolean getBlockOnBufferFull() {
        return blockOnBufferFull;
    }

    public void setBlockOnBufferFull(boolean blockOnBufferFull) {
        this.blockOnBufferFull = blockOnBufferFull;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public Integer getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(Integer retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(Integer lingerMs) {
        this.lingerMs = lingerMs;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public Integer getRetries() {
        return retries;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public Integer getReconnectBackoffMs() {
        return reconnectBackoffMs;
    }

    public void setReconnectBackoffMs(Integer reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
    }
}
