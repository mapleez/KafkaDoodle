package org.dt.ez.kafka.tools.complex;

import java.util.List;
import java.util.Properties;

import org.dt.ez.kafka.tools.former.ConsumerService;

public class ConsumerWorker implements Runnable {
	
	private ConsumerService consumer;
	private long workerId;
	
	public ConsumerWorker (Properties props) {
		consumer = new ConsumerService (props);
	}
	
	public ConsumerWorker setWorkerId (long workerId) {
		this.workerId = workerId;
		return this;
	}

	@Override
	public void run () {
		while (true) {
			consumeMessage (consumer);
			finishedConsume (consumer);
		}
	}
	
	
	private void finishedConsume (ConsumerService consumer) {
		consumer.commitSync ();
	}
	
	
	private void consumeMessage (ConsumerService consumer) {
		List <String> msgs = consumer.getMessages ();
		for (String msg : msgs)
			System.out.println (workerId  + " " + msg);
	}
	
}


