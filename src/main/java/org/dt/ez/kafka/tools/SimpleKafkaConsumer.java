package org.dt.ez.kafka.tools;

import java.util.Properties;

import org.dt.ez.kafka.tools.former.ConsumerService;

import java.util.List;

public class SimpleKafkaConsumer {

	
	/**
	 * 
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> group.id
	 *  args [3] -> SASL file
	 */
	public static void main (String [] args) {
		
		if (args.length < 3) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <groupid> [<SASLfile>].");
			System.exit (1);
		}

		Properties props = new Properties ();
		props.setProperty ("topics", args [1]);
		
		props.setProperty ("bootstrap.servers", args [0]);
		props.setProperty ("group.id", args [2]);
		props.setProperty ("enable.auto.commit", "false");
		props.setProperty ("auto.commit.interval.ms", "2000");
		props.setProperty ("session.timeout.ms", "30000");
		props.setProperty ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		// Using JAAS file.
		if (args.length > 3) {		
			/* For SASL */
			props.setProperty ("security.protocol", "SASL_PLAINTEXT");
			props.setProperty ("sasl.mechanism", "PLAIN");
			
			/* SASL JAAS file. */
			System.setProperty ("java.security.auth.login.config", args [3]);
		}
		
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
