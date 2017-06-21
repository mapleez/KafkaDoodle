package org.dt.ez.kafka;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerService <K, V> {
	
	private KafkaConsumer <K, V> consumer;
	private List <ConsumerRecord <K, V>> buffer;
	
	private long pollTm;
	private int buffSize;
	
	public ConsumerService (Properties props) {
		
		initConfigurable (props);
		
		consumer = new KafkaConsumer <K, V> (props);
		consumer.subscribe (splitTopics2Collection (
			props.getProperty (ConfigKey.KAFKA_CONSUMER_TOPICS, "eztest")));
		buffer = new ArrayList <ConsumerRecord <K, V>> (buffSize);
	}
	
	public ConsumerService (Properties props, List <ConsumerRecord <K, V>> outputBuffer) {
		
		initConfigurable (props);
		
		consumer = new KafkaConsumer <K, V> (props);
		consumer.subscribe (splitTopics2Collection (
			props.getProperty (ConfigKey.KAFKA_CONSUMER_TOPICS, "eztest")));
		buffer = outputBuffer;
	}
	
	private void initConfigurable (Properties props) {
		pollTm = Long.parseLong (props.getProperty (ConfigKey.KAFKA_CONSUMER_POLL_TIME, 
			Constance.DEF_KAFKA_CONSUMER_POLL_TIME));
		buffSize = Integer.parseInt (props.getProperty (ConfigKey.KAFKA_CONSUMER_BUFFSIZE,
			Constance.DEF_KAFKA_CONSUMER_BUFSIZE));
	}
	
	private Collection <String> splitTopics2Collection (String topicsString) {
		return splitTopics2Collection (topicsString, ",");
	}
	
	private Collection <String> splitTopics2Collection (String topicsString, String separator) {
		Collection <String> topics = new ArrayList <String> ();
		Collections.addAll (topics, topicsString.split (separator));
		return topics;
	}
	
	public List <ConsumerRecord <K, V>> getMessages () {
		ConsumerRecords <K, V> records = consumer.poll (this.pollTm);
		for (ConsumerRecord <K, V> record : records)
			buffer.add (record);
		return buffer;
	}
	
	public List <ConsumerRecord <K, V>> commitSync () {
		buffer.clear ();
		consumer.commitSync ();
		return buffer;
	}
	
	public static void main (String [] args) {
		Properties props = getProperties ();
		
		List <ConsumerRecord <String, String>> buffer = 
			new ArrayList <ConsumerRecord <String, String>> (1024);
		
		ConsumerService <String, String> consumer = 
			new ConsumerService <String, String> (props, buffer);
		
		do {
			consumer.getMessages ();
			displayMessage (buffer);
			consumer.commitSync ();
		} while (true);
	}
	
	private static void displayMessage (List <ConsumerRecord <String, String>> output) {
		for (ConsumerRecord <String, String> record : output) {
			System.out.printf ("%s => %s\n", record.key (), record.value ());
		}
	}
	
	private static Properties getProperties () {
		Properties props = new Properties ();
		InputStream inStream = ConsumerService.class.getClassLoader()
			.getResourceAsStream ("config.properties");
		try {
			props.load (inStream);
		} catch (IOException e) {
			e.printStackTrace ();
		}
		
		return props;
//		props.setProperty (ConfigKey.KAFKA_CONSUMER_TOPICS, "iot");
//		props.setProperty (ConfigKey.KAFKA_CONSUMER_POLL_TIME, "100");
//		props.setProperty (ConfigKey.KAFKA_BOOTSTRAP_LIST, "M1:9092");
//		props.setProperty (ConfigKey.KAFKA_VALUE_DESERIALIZER, 
//			"org.apache.kafka.common.serialization.StringDeserializer");
//		props.setProperty (ConfigKey.KAFKA_KEY_DESERIALIZER, 
//			"org.apache.kafka.common.serialization.StringDeserializer");
//		props.setProperty (ConfigKey.KAFKA_GROUP_ID, "eztests");
	}
	
}



