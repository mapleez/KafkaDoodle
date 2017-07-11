package org.dt.ez.kafka.tools.former;

import java.util.Properties;

public class Consumer {
	
	private static String bootstrapServer = 
			"vcentosez2:9092,vdebianez3:9092,vubuntuez1:9092";
	

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put ("bootstrap.servers", bootstrapServer);
		props.put ("group.id", "eztestGrp");
		props.put ("enable.auto.commit", "false");
		props.put ("auto.commit.interval.ms", "2000");
//		props.put ("session.timeout.ms", "30000");
		props.put ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		PrintConsumerController consumer = new PrintConsumerController (props);
		consumer.run ();
		
	}

}
