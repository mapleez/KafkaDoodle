/**
 * Author : ez
 * Date : 2017/7/11
 * Describe : A simple Consumer client program for kafka. 
 *     With configurable multiple thread.
 */
package org.dt.ez.kafka.tools.complex;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultipleThreadKafkaConsumer {
	
	private static int threadNum;
	
	/**
	 * Thread pool for all handle worker.
	 * We use fixed thread pool.
	 */
	private static ExecutorService threadPool;
	
	/**
	 * All registered worker (thread).
	 */
	private static ConsumerWorker [] workers;
	
	/**
	 * Configuration
	 */
	private static Properties props;

	/**
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> group.id
	 *  args [3] -> thread number
	 *  args [4] -> SASL file
	 */
	public static void main (String [] args) {
		
		if (args.length < 4) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <groupid> <thread_num> [<SASLfile>].");
			System.exit (1);
		}

		props = new Properties ();
		props.setProperty ("topics", args [1]);
		
		props.setProperty ("bootstrap.servers", args [0]);
		props.setProperty ("group.id", args [2]);
		props.setProperty ("enable.auto.commit", "false");
		props.setProperty ("auto.commit.interval.ms", "2000");
		props.setProperty ("session.timeout.ms", "30000");
		props.setProperty ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		threadNum = Integer.parseInt (args [3]);
		if (threadNum <= 0) threadNum = 1;
		
		threadPool = Executors.newFixedThreadPool (threadNum);
		workers = new ConsumerWorker [threadNum];
		
		// Using JAAS file.
		if (args.length > 4) {
			/* For SASL */
			props.setProperty ("security.protocol", "SASL_PLAINTEXT");
			props.setProperty ("sasl.mechanism", "PLAIN");
			
			/* SASL JAAS file. */
			System.setProperty ("java.security.auth.login.config", args [4]);
		}
		
		initThreads ();
		startThreads ();
	}
	
	private static void startThreads () {
		for (ConsumerWorker worker : workers)
			threadPool.execute (worker);
		threadPool.shutdown ();
	}
	
	private static void initThreads () {
		for (int i = 0; i < threadNum; ++ i) {
			workers [i] = new ConsumerWorker (props);
			workers [i].setWorkerId (i + 1);
		}
	}
	
}
