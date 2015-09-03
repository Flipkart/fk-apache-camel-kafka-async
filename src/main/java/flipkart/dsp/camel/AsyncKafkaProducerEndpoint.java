/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flipkart.dsp.camel;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

@UriEndpoint(scheme = "kafkaasync", title = "KafkaAsync", syntax = "kafkaasync:brokers",
        label = "messaging", producerOnly = true)
public class AsyncKafkaProducerEndpoint extends DefaultEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncKafkaProducerEndpoint.class);

    @UriPath
    @Metadata(required = "true")
    private String brokers;
    @UriParam
    private AsyncKafkaProducerConfiguration configuration = new AsyncKafkaProducerConfiguration();

    private final String endpointUri;

    public AsyncKafkaProducerEndpoint(String endpointUri,
                                      String remaining,
                                      AsyncKafkaComponent component) throws URISyntaxException {
        super(endpointUri, component);
        this.brokers = remaining.split("\\?")[0];
        this.endpointUri = endpointUri;
        LOG.info("Creating Endpoint - [" + endpointUri + "]");

    }

    public AsyncKafkaProducerConfiguration getConfiguration() {
        if (configuration == null) {
            configuration = createConfiguration();
        }
        return configuration;
    }

    public void setConfiguration(AsyncKafkaProducerConfiguration configuration) {
        this.configuration = configuration;
    }

    protected AsyncKafkaProducerConfiguration createConfiguration() {
        return new AsyncKafkaProducerConfiguration();
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return null;
    }

    @Override
    protected String createEndpointUri() {
        return endpointUri;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new AsyncKafkaProducer(this);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public String getTopic() {
        return configuration.getTopic();
    }

    public void setTopic(String topic) {
        configuration.setTopic(topic);
    }

    public String getBrokers() {
        return brokers;
    }

    public int getBatchSize() {
        return configuration.getBatchSize();
    }

    public void setBatchSize(Integer batchSize) {

        this.configuration.setBatchSize(batchSize);
    }

    public void setLingerMs(Integer lingerMs) {
        this.configuration.setLingerMs(lingerMs);
    }

    public int getLingerMs() {
        return this.configuration.getLingerMs();
    }

    public void setAcks(String acks) {
        this.configuration.setAcks(acks);
    }

    public String getAcks() {
        return this.configuration.getAcks();
    }

    public void setKeySerializer(String serializer) {
        this.configuration.setKeySerializer(serializer);
    }

    public String getKeySerializer() {
        return this.configuration.getKeySerializer();
    }

    public String getValueSerializer() {
        return this.configuration.getValueSerializer();
    }

    public String getClientId() {
        return configuration.getClientId();
    }

    public void setValueSerializer(String valueSerializer) {
        this.configuration.setValueSerializer(valueSerializer);
    }

    public int getRetryBackoffMs() {
        return configuration.getRetryBackoffMs();
    }

    public void setRetryBackoffMs(Integer retryBackOfMs) {
        configuration.setRetryBackoffMs(retryBackOfMs);
    }

    public void setClientId(String clientId) {
        configuration.setClientId(clientId);
    }

    public Integer getRetries() {
        return configuration.getRetries();
    }

    public void setRetries(Integer retries) {
        configuration.setRetries(retries);
    }
}
