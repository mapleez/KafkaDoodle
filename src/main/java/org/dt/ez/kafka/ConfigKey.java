package org.dt.ez.kafka;

public final class ConfigKey {
	
	public final static String KAFKA_CONSUMER_TOPICS = "kafka.consumer.topics";
	
	public final static String KAFKA_PRUDUCER_TOPICS = "kafka.producer.topics";
	
	public final static String KAFKA_CONSUMER_POLL_TIME = "kafka.consumer.poll.time";
	
	public final static String KAFKA_CONSUMER_BUFFSIZE = "kafka.consumer.buffsize";
	
	public final static String KAFKA_BOOTSTRAP_LIST = "bootstrap.servers";
	
	public final static String KAFKA_VALUE_DESERIALIZER = "value.deserializer";
	
	public final static String KAFKA_KEY_DESERIALIZER = "key.deserializer";
	
	public final static String KAFKA_GROUP_ID = "group.id";
}
