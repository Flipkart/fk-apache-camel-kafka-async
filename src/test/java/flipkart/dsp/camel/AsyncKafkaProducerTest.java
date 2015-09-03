package flipkart.dsp.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URISyntaxException;
import java.util.Properties;

import static flipkart.dsp.camel.ComponentTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bhushan.sk on 31/08/15.
 */
public class AsyncKafkaProducerTest {

    private AsyncKafkaProducer kafkaProducer;
    private AsyncKafkaProducerEndpoint kafkaProducerEndpoint;
    CamelContext context = new DefaultCamelContext();

    private Exchange exchange = mock(Exchange.class);
    private Message in = new DefaultMessage();

    public AsyncKafkaProducerTest() throws URISyntaxException {
        kafkaProducerEndpoint = new AsyncKafkaProducerEndpoint(URI, BROKERS, new AsyncKafkaComponent());

        kafkaProducer = new AsyncKafkaProducer(kafkaProducerEndpoint);
        kafkaProducer.producer = mock(Producer.class);
    }

    @Test
    public void testIfPropertiesBeingSetCorrectly() {
        // set something...
        this.kafkaProducerEndpoint.setValueSerializer("fooSerializer");
        this.kafkaProducerEndpoint.setClientId(CLIENT_ID);

        Properties properties = kafkaProducer.getProps();
        // retrieve them back...
        assertThat(properties.getProperty(VALUE_SERIALIZER_KAFKA_STR), is("fooSerializer"));
        assertThat(properties.getProperty(BOOTSTRAP_SERVERS_STR), is(BOOTSTRAP_SERVERS));
        assertThat(properties.getProperty(CLIENT_ID_STR),is(CLIENT_ID));

    }

    @Test
    public void testSendMessageWithPartitionKey() throws Exception {
        in.setHeader(KafkaConstants.PARTITION_KEY, PARTITION_KEY_VALUE);
        sendMessageViaKafkaLowlevelKafkaProducer();
        verifySendMessage(PARTITION_KEY_VALUE, TOPIC);
    }

    @Test
    public void testSendMessageWithoutPartitionKey() throws Exception {

        sendMessageViaKafkaLowlevelKafkaProducer();
        verify(kafkaProducer.producer, times(1)).send(Matchers.any(ProducerRecord.class));
        verifyNoMoreInteractions(kafkaProducer.producer);
        verifySendMessage(null, TOPIC);
    }


    void sendMessageViaKafkaLowlevelKafkaProducer() throws Exception {

        this.kafkaProducerEndpoint.setTopic(TOPIC);
        in.setBody("My name is foo");
        when(exchange.getIn()).thenReturn(in);
        this.kafkaProducer.process(exchange);

        verify(kafkaProducer.producer, times(1)).send(Matchers.any(ProducerRecord.class));
        verifyNoMoreInteractions(kafkaProducer.producer);
    }

    protected void verifySendMessage(String partitionKey, String topic) {
        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        Mockito.verify(kafkaProducer.producer).send(captor.capture());
        assertEquals(partitionKey, captor.getValue().key());
        assertEquals(topic, captor.getValue().topic());
    }
}