package org.dt.ez.kafka.tools.complex;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultipleThreadKafkaProducer {
	
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
	 * Datas.
	 */
	private static String [] datas;

	/**
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> data file
	 *  args [3] -> thread number
	 *  args [4] -> SASL file
	 */
	public static void main (String [] args) {
		if (args.length < 4) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <data_file> <thread_num> [<SASLfile>].");
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
		
		datas = DataStrategy.parseFromArgument (args [2]);
		threadNum = Integer.parseInt (args [3]);
		if (threadNum <= 0) threadNum = 0;
		
		threadPool = Executors.newFixedThreadPool (threadNum);
		workers = new ProducerWorker [threadNum];
		
		/* For SASL */
		if (args.length > 4) {
			/* For SASL configuration*/
			prop.setProperty ("security.protocol", "SASL_PLAINTEXT");
			prop.setProperty ("sasl.mechanism", "PLAIN");
			
			/* SASL JAAS file. */
			System.setProperty ("java.security.auth.login.config", args [4]);
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
			workers [i].datas = datas;
		}
	}
}


