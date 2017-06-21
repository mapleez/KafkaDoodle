package com.joinbright.kafka.tools;

import java.util.Properties;

import com.joinbright.alarmer.service.ConsumerService;

import java.util.List;

public class SimpleKafkaConsumer {

	public static void main (String [] args) {

		Properties props = new Properties ();
		props.setProperty ("topics", "eztest");
		
		props.setProperty ("bootstrap.servers", "vubuntuez1:9092");
		props.setProperty ("group.id", "dbg_grp");
		props.setProperty ("enable.auto.commit", "false");
		props.setProperty ("auto.commit.interval.ms", "2000");
		props.setProperty ("session.timeout.ms", "30000");
		props.setProperty ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		/* For SASL */
		props.setProperty ("security.protocol", "SASL_PLAINTEXT");
		props.setProperty ("sasl.mechanism", "PLAIN");
		
		/* SASL JAAS file. */
		System.setProperty ("java.security.auth.login.config", "conf/kafka_client_jaas.conf");
		
		ConsumerService consumer = new ConsumerService (props);
		while (true) {
			consumeMessage (consumer);
			finishedConsume (consumer);
		}
	}
	
	public static void finishedConsume (ConsumerService consumer) {
		consumer.commitSync ();
	}
	
	public static void consumeMessage (ConsumerService consumer) {
		List <String> msgs = consumer.getMessages ();
		for (String msg : msgs) {
			System.out.println (msg);
		}
	}
	

}
