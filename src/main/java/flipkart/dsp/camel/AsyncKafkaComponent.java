
package flipkart.dsp.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.UriEndpointComponent;

import java.util.Map;


public class AsyncKafkaComponent extends UriEndpointComponent {

    public AsyncKafkaComponent() {
        super(AsyncKafkaProducerEndpoint.class);
    }

    public AsyncKafkaComponent(CamelContext context) {
        super(context, AsyncKafkaProducerEndpoint.class);
    }

    @Override
    protected AsyncKafkaProducerEndpoint createEndpoint(String uri,
                                                 String remaining,
                                                 Map<String, Object> params) throws Exception {
        AsyncKafkaProducerEndpoint endpoint = new AsyncKafkaProducerEndpoint(uri, remaining, this);
        setProperties(endpoint, params);
        return endpoint;
    }
}
