/**
 * WARNINGS : 
 * 	It's just a temporary ProducerWorker.
 * 	Original code is in package org.dt.ez.kafka.tools.complex
 */
package org.dt.ez.kafka.doodle.partitions;

import java.util.Properties;
import org.apache.log4j.Logger;
import org.dt.ez.kafka.tools.former.ProducerEngine;

import org.dt.ez.common.utils.Pair;

public class ProducerWorker implements Runnable {

	private ProducerEngine engine;
	private SequenceData data;
	private int workerID;
	
	private Pair <String, String> [] buffer;
	private static int bufferLength = 500;
	private static Logger logger = Logger.getLogger (ProducerWorker.class);
	
	@SuppressWarnings ("unchecked")
	public ProducerWorker (Properties props) {
		engine = new ProducerEngine (props);
		buffer = (Pair <String, String> []) new Pair [bufferLength];
		for (int i = 0; i < bufferLength; ++ i)
			buffer [i] = new Pair <String, String> ();
	}
	
	public ProducerWorker setWorkerID (int id) {
		this.workerID = id;
		return this;
	}
	
	public void setData (SequenceData data) {
		this.data = data;
	}

	@Override
	public void run () {
		while (true) {
			logger.debug ("Start sending (" + buffer.length + 
				") messages [" + System.currentTimeMillis () + "].");
			constructDataIntoBuffer ();
			engine.send (buffer);
			logger.debug ("finish sending (" + buffer.length + 
				") messages [" + System.currentTimeMillis () + "].");
			sleep (1000);
		}
	}
	
	private void constructDataIntoBuffer () {
		for (int i = 0; i < bufferLength; i ++)
			data.getKafkaValue (buffer [i]);
	}
	
	
	private void sleep (long ms) {
		try {
			Thread.sleep (ms);
		} catch (Exception ex) {
			ex.printStackTrace ();
		}
	}
	
}
