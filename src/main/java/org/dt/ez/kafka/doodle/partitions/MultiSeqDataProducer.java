package org.dt.ez.kafka.doodle.partitions;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import org.dt.ez.kafka.tools.complex.DataStrategy;

public class MultiSeqDataProducer {
	
	private static int threadNum;
	
	/**
	 * Thread pool for all handle worker.
	 * We use fixed thread pool.
	 */
	private static ExecutorService threadPool;
	
	/**
	 * All registered worker (thread).
	 */
	private static ProducerWorker [] workers;
	
	/**
	 * Configuration
	 */
	private static Properties prop;
	

	/**
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> thread number
	 *  args [3] -> SASL file
	 */
	public static void main (String [] args) {
		if (args.length < 3) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <thread_num> [<SASLfile>].");
			System.exit (1);
		}
		
		prop = new Properties ();
		prop.put ("producer.topic", args [1]);
		prop.put ("producer.bootstrap.servers", args [0]);
		
		prop.put ("acks", "all");
		prop.put ("retries", 0);
		prop.put ("batch.size", 16384);
		prop.put ("linger.ms", 0);
		prop.put ("buffer.memory", 33554432);
		prop.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		threadNum = Integer.parseInt (args [2]);
		if (threadNum <= 0) threadNum = 1;
		
		threadPool = Executors.newFixedThreadPool (threadNum);
		workers = new ProducerWorker [threadNum];
		
		/* For SASL */
		if (args.length > 3) {
			/* For SASL configuration*/
			prop.setProperty ("security.protocol", "SASL_PLAINTEXT");
			prop.setProperty ("sasl.mechanism", "PLAIN");
			
			/* SASL JAAS file. */
			System.setProperty ("java.security.auth.login.config", args [3]);
		}
		
		initThreads ();
		startThreads ();

	}
	
	private static void startThreads () {
		for (ProducerWorker worker : workers)
			threadPool.execute (worker);
		threadPool.shutdown ();
	}
	
	private static void initThreads () {
		for (int i = 0; i < threadNum; ++ i) {
			workers [i] = new ProducerWorker (prop);
			int workerID = i * 3;
			workers [i].setWorkerID (workerID);
			
			SequenceData dataSource = new SequenceData (10)
				.registerTag ("A" + workerID)
				.registerTag ("A" + (workerID + 1))
				.registerTag ("A" + (workerID + 2));
			
			workers [i].setData (dataSource);
		}
	}
}



