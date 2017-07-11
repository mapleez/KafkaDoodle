package org.dt.ez.kafka.tools.former;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerService {

	private KafkaConsumer <String, String> consumer;
	private int batchSize;
	private int pollTime;

	public ConsumerService (Properties props) {
		this.consumer = ConsumerFactory.create (props);
		this.batchSize = 1;
		this.pollTime = 100;
	}

	public ConsumerRecords<String, String> poll (int ms) {
		return consumer.poll (ms);
	}

	/**
	 * Get Kafka message data model
	 * 
	 * @return
	 */
	public List <String> getMessages () {
		List <String> messages = new ArrayList <String> ();
		// Get Record
		do {
			ConsumerRecords<String, String> records = consumer.poll (pollTime);
			for (ConsumerRecord <String, String> record : records) {
				messages.add (record.value ());
			}
		} while (messages.size () < batchSize);
		return messages;
	}

	public void commitSync() {
		consumer.commitSync();
	}

}
