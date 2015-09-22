package flipkart.dsp.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.HashMap;
import java.util.Map;

import static flipkart.dsp.camel.ComponentTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;

public class AsyncKafkaComponentTest extends CamelTestSupport {

    private CamelContext camelContext = mock(CamelContext.class);

    @Test
    public void testCheckIfPropertiesAreSetForTheEndpoint() throws Exception {

        Map<String, Object> params = new HashMap<>();
        params.put(TOPIC_STR, TOPIC);
        params.put(BATCH_SIZE_STR,BATCH_SIZE);
        params.put(LINGER_MS_STR, LINGER_MS);
        params.put(ACKS_STR,ACKS);
        params.put(BROKERS_STR,BROKERS);
        params.put(KEY_SERIALIZER_STR, KEY_SERIALIZER);
        params.put(VALUE_SERIALIZER_STR,VALUE_SERIALIZER);
        params.put(RETRY_BACKOFF_STR, RETRY_BACKOFF);

        AsyncKafkaProducerEndpoint endpoint = new AsyncKafkaComponent(camelContext).
                createEndpoint(URI, BROKERS, params);

        assertThat(endpoint.getBatchSize(), is(BATCH_SIZE));
        assertThat(endpoint.getLingerMs(), is(LINGER_MS));
        assertThat(endpoint.getBrokers(), is(BROKERS));
        assertThat(endpoint.getAcks(),is(ACKS));
        assertThat(endpoint.getKeySerializer(), is(KEY_SERIALIZER));
        assertThat(endpoint.getValueSerializer(), is(VALUE_SERIALIZER));
        assertThat(endpoint.getTopic(),is(TOPIC));
        assertThat(endpoint.getRetryBackoffMs(), is(RETRY_BACKOFF));
    }
}
