package flipkart.dsp.camel;

import org.apache.camel.CamelException;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.PrintStream;
import java.io.StringWriter;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by bhushan.sk on 26/08/15.
 */
public class AsyncKafkaProducer extends DefaultProducer {

    private final AsyncKafkaProducerEndpoint endpoint;
    protected Producer producer;

    // This value governs, when the caling thread 'times out', over the Kafka Future
    // irrespective of the (retryInterval * retries) value.
    final static Integer maxAsyncAwaitInMs = 2000;

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
        Integer maxAsyncWait = exchange.getIn().getHeader(KafkaConstants.MAX_ASYNC_WAIT, Integer.class);
        if (maxAsyncWait == null)
            maxAsyncWait = new Integer(maxAsyncAwaitInMs);

        boolean hasPartitionKey = partitionKey != null;

        String msg = exchange.getIn().getBody(String.class);
        if (!hasPartitionKey) {
            log.warn("No message key or partition key set");
        }
        try {

            Future<RecordMetadata> recordMetadataFuture = null;

            log.debug("AsyncProducer:Record about to be sent");
            if (hasPartitionKey)
                recordMetadataFuture = this.producer.send(
                        new ProducerRecord(topic, partitionKey, msg));
            else
                recordMetadataFuture =
                        this.producer.send(new ProducerRecord(topic, msg));

            if (recordMetadataFuture != null) {
                RecordMetadata metadata = recordMetadataFuture.get(maxAsyncWait, TimeUnit.MILLISECONDS);
                if (metadata != null)
                    log.debug("AsyncProducer:Record sent. metadata is = " + metadata.offset() + "topic =" + metadata.topic() + "partition="
                            + metadata.partition());
            }
        } catch (Exception ex) {
            //log.error("Exception - " + ex.toString() + ex.getMessage());
            throw new CamelExchangeException("failed to send message after " + maxAsyncWait.toString() + "ms timeout",
                    exchange, ex);
        }
    }
}
