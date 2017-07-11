package org.dt.ez.kafka.tools.former;

import java.util.List;
import java.util.Properties;

public abstract class ConsumerController implements Runnable {

//	static Logger logger = Logger.getLogger(ConsumerController.class);
	private ConsumerService consumer;

	public ConsumerController (Properties properties) {
		this.consumer = new ConsumerService (properties);
	}

	public void run() {
		while (true) {
			try {
				List<String> messages = consumer.getMessages ();
				dealWithMessage (messages);
				consumer.commitSync ();
			} catch (Exception e) {
				e.printStackTrace ();
			}
		}
	}

	protected abstract void dealWithMessage (List<String> messages);

}
