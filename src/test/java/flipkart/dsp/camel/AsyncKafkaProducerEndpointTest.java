package flipkart.dsp.camel;

import org.junit.Test;

import java.net.URISyntaxException;

import static flipkart.dsp.camel.ComponentTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by bhushan.sk on 31/08/15.
 */
public class AsyncKafkaProducerEndpointTest {

    @Test
    public void assertSingleton() throws URISyntaxException {
        AsyncKafkaProducerEndpoint endpoint = new AsyncKafkaProducerEndpoint("kafka:localhost",
                "localhost", new AsyncKafkaComponent());
        assertTrue(endpoint.isSingleton());
    }

    @Test
    public void testGetAndSetEndpointConfiguration() throws Exception{

        AsyncKafkaProducerConfiguration configuration = new AsyncKafkaProducerConfiguration();
        configuration.setLingerMs(LINGER_MS);
        configuration.setBatchSize(BATCH_SIZE);
        configuration.setValueSerializer(VALUE_SERIALIZER);
        configuration.setKeySerializer(KEY_SERIALIZER);
        configuration.setAcks(ACKS);
        configuration.setTopic(TOPIC);
        configuration.setRetryBackoffMs(RETRY_BACKOFF);

        AsyncKafkaProducerEndpoint endpoint = new AsyncKafkaProducerEndpoint("kafka:localhost", "localhost",
                    new AsyncKafkaComponent())  ;

        endpoint.setConfiguration(configuration);

        assertThat(endpoint.getLingerMs(), is(LINGER_MS));
        assertThat(endpoint.getKeySerializer(), is(KEY_SERIALIZER));
        assertThat(endpoint.getBatchSize(), is(BATCH_SIZE));
        assertThat(endpoint.getValueSerializer(), is(VALUE_SERIALIZER));
        assertThat(endpoint.getAcks(), is(ACKS));
        assertThat(endpoint.getTopic(), is(TOPIC));
        assertThat(endpoint.getRetryBackoffMs(), is(RETRY_BACKOFF));

    }
}