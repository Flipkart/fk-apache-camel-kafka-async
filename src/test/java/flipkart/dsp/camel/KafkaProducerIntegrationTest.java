package flipkart.dsp.camel;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.camel.CamelContext;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static flipkart.dsp.camel.ComponentTestConstants.TOPIC;
import static org.hamcrest.core.Is.is;

/**
 * Created by bhushan.sk on 01/09/15.
 * Integration test to test e2e KafkaProducer, by producing the message and consuming via KafkaConsumer
 * itself
 * It needs the Kafka server and Zookeeper to be installed on the localhost, as pre-req
 */
public class KafkaProducerIntegrationTest extends CamelTestSupport {

    private static final String GROUP_ID = "group_x";

    private static final int MESSAGES_TO_BE_SENT_TOPIC = 20;

    private static final String MESSAGE_BODY = "This is ABRACADABRA";
    private static final String PARTITION_KEY = "FooPartitionKey";
    private static final String TEST_ROUTE = "test_route" ;

    private static ConsumerConnector stringsConsumerConn;

    @Produce(uri = "direct:startEp")
    private ProducerTemplate  startEndpoint;

    private CamelContext camelContext = new DefaultCamelContext();

    @Override
    public CamelContext createCamelContext() {
        return camelContext;
    }

    @Override
    public RouteBuilder createRouteBuilder() {

        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct://startEp").to(ComponentTestConstants.KAFKA_TO_URL).routeId(TEST_ROUTE);
            }
        };
    }

    @Test
    public void shouldValidateMaxAsyncWaitParameterViaExchange() throws Exception {
        final RouteDefinition route = camelContext.getRouteDefinition(TEST_ROUTE);
        MutableBoolean routeIntercepted = new MutableBoolean(false);
        final Integer[] maxAwait = new Integer[1];
        route.adviceWith(camelContext, new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                interceptSendToEndpoint(ComponentTestConstants.KAFKA_TO_URL)
                        .skipSendToOriginalEndpoint()
                        .process(exchange -> {
                            routeIntercepted.setTrue();
                            maxAwait[0] = exchange.getIn().getHeader(KafkaConstants.MAX_ASYNC_WAIT, Integer.class);
                        });
            }
        });
        try {

            Map<String, Object> inTopicHeaders = new HashMap<String, Object>();
            inTopicHeaders.put(KafkaConstants.PARTITION_KEY, PARTITION_KEY.getBytes());
            inTopicHeaders.put(KafkaConstants.MAX_ASYNC_WAIT,ComponentTestConstants.MAXAWAITASYNC);

            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(TOPIC, 5);

            this.startEndpoint.sendBodyAndHeaders(MESSAGE_BODY, inTopicHeaders);

        } catch (RuntimeCamelException e) {
            // Expected
        }
        assertTrue(routeIntercepted.booleanValue());
        assertThat(maxAwait[0],is(ComponentTestConstants.MAXAWAITASYNC));
    }

    @Before
    public void before() {
        Properties stringsProps = new Properties();

        stringsProps.put("zookeeper.connect", "localhost:" + 2181);
        stringsProps.put("group.id", GROUP_ID);
        stringsProps.put("zookeeper.session.timeout.ms", "6000");
        stringsProps.put("zookeeper.connectiontimeout.ms", "12000");
        stringsProps.put("zookeeper.sync.time.ms", "200");
        stringsProps.put("auto.commit.interval.ms", "1");
        stringsProps.put("auto.offset.reset", "smallest");
        stringsConsumerConn = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(stringsProps));
    }

    @After
    public void cleanUp(){
        if (stringsConsumerConn != null)
            stringsConsumerConn.shutdown();
    }

    @Test
    public void testSendMessageToDirectAndVerifyByConsumingFromKafka() throws Exception {

        // Wait for the all the message, to be received by the consumer, which are produced
        // by KafkaProducer
        CountDownLatch messagesLatch = new CountDownLatch(MESSAGES_TO_BE_SENT_TOPIC);

        Map<String, Object> inTopicHeaders = new HashMap<String, Object>();
        inTopicHeaders.put(KafkaConstants.PARTITION_KEY, PARTITION_KEY.getBytes());
        inTopicHeaders.put(KafkaConstants.MAX_ASYNC_WAIT, 5000);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, 5);

        createKafkaMessageConsumer(stringsConsumerConn, TOPIC, messagesLatch, topicCountMap);

        sendMessagesInRoute(MESSAGES_TO_BE_SENT_TOPIC, this.startEndpoint, MESSAGE_BODY, inTopicHeaders);

        boolean allMessagesReceived = messagesLatch.await(40000, TimeUnit.MILLISECONDS);

        assertTrue("Not all messages were published to the kafka topics. Not received: " +
                messagesLatch.getCount(), allMessagesReceived);
    }

    private void sendMessagesInRoute(int messages, ProducerTemplate template,
                                     String bodyOther, Map<String, Object> headerMap) {
        for (int k = 0; k < messages; k++) {
            template.sendBodyAndHeaders(bodyOther, headerMap);
        }
    }

    private void createKafkaMessageConsumer(ConsumerConnector consumerConn, String topic,
                                            CountDownLatch messagesLatch, Map<String, Integer> topicCountMap) {

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConn.createMessageStreams(topicCountMap);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
            executor.submit(new KakfaTopicConsumer(stream, messagesLatch));
        }
    }

    private static class KakfaTopicConsumer implements Runnable {
        private final KafkaStream<byte[], byte[]> stream;
        private final CountDownLatch latch;

        public KakfaTopicConsumer(KafkaStream<byte[], byte[]> stream, CountDownLatch latch) {
            this.stream = stream;
            this.latch = latch;
        }

        @Override
        public void run() {

            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            int i = 0;
            while (it.hasNext()) {
                String msg = new String(it.next().message());
                latch.countDown();
            }
        }
    }
}

