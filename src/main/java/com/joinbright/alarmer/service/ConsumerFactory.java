package com.joinbright.alarmer.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerFactory {

	static Collection<String> getTopics (String topicString) {
		Collection <String> topics = new ArrayList <String> ();
		Collections.addAll (topics, topicString.split (","));
		return topics;
	}

	public static KafkaConsumer <String, String> create (Properties properties) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer <String, String> (properties);
		consumer.subscribe (getTopics (properties.getProperty ("topics", "eztest")));
		return consumer;
	}
	
	

}
