# fk-camel-kafka-async

New component - Camel Kafka Producer, which uses Kafka Asynchronous.
The existing Camel-Kafka 2.15 component, uses single low level Producer
The kafka.javaapi.Producer holds a global lock, before sending every single message

The new 0.8.2.x Async Producer by Kafka Java API, let ones tune the locking and latency by defining
the batchSize and lingering time as configurables. linger.ms = 0, means as good as flushing the message
buffer as soon as it is received.
