package flipkart.dsp.camel;

import org.apache.camel.CamelException;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by bhushan.sk on 26/08/15.
 */
public class AsyncKafkaProducer extends DefaultProducer {

    private final AsyncKafkaProducerEndpoint endpoint;
    protected Producer producer;

    public AsyncKafkaProducer(AsyncKafkaProducerEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStop() throws Exception {
        if (this.producer != null)
            this.producer.close();
    }

    @Override
    protected void doStart() throws Exception {
        Properties props = getProps();
        producer = createProducer(props);
    }

    protected Producer createProducer(Properties properties) {
        return new KafkaProducer(properties);
    }

    protected Properties getProps() {
        Properties properties =
                this.endpoint.getConfiguration().createProducerProperties();
        properties.put("bootstrap.servers", endpoint.getBrokers());
        return properties;
    }

    @Override
    public void process(Exchange exchange) throws CamelException {

        String topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, endpoint.getTopic(), String.class);
        if (topic == null) {
            throw new CamelExchangeException("No topic key set", exchange);
        }
        String partitionKey = exchange.getIn().getHeader(KafkaConstants.PARTITION_KEY, String.class);
        boolean hasPartitionKey = partitionKey != null;

        String msg = exchange.getIn().getBody(String.class);
        if (!hasPartitionKey) {
            log.warn("No message key or partition key set");
        }
        try {

            Future<RecordMetadata> recordMetadataFuture = null;

            if (hasPartitionKey)
                recordMetadataFuture = this.producer.send(
                        new ProducerRecord(topic, partitionKey, msg));
            else
                recordMetadataFuture = this.producer.send(new ProducerRecord(topic, msg));

            if (recordMetadataFuture != null) {
                RecordMetadata metadata = recordMetadataFuture.get(); // block on this;
                log.info("Record metadata isDone = " + metadata.offset() + "topic =" + metadata.topic());
            }
        } catch (Exception ex) {
            throw new CamelException(ex);
        }
    }
}
