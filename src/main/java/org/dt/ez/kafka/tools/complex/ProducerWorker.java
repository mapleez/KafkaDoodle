package org.dt.ez.kafka.tools.complex;

import java.util.Properties;
import org.apache.log4j.Logger;
import org.dt.ez.kafka.tools.former.ProducerEngine;

public class ProducerWorker implements Runnable {

	private ProducerEngine engine;
	public static String [] datas;
	private static Logger logger = Logger.getLogger (ProducerWorker.class);
	
	public ProducerWorker (Properties props) {
		engine = new ProducerEngine (props);
	}

	@Override
	public void run () {
		while (true) {
			logger.debug ("Start sending (" + datas.length + 
				") messages [" + System.currentTimeMillis () + "].");
			engine.send (datas);
			logger.debug ("finish sending (" + datas.length + 
				") messages [" + System.currentTimeMillis () + "].");
			sleep (5000);
		}
	}
	
	private void sleep (long ms) {
		try {
			Thread.sleep (ms);
		} catch (Exception ex) {
			ex.printStackTrace ();
		}
	}
	
}
