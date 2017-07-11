package org.dt.ez.kafka.tools;

import java.util.Properties;

import org.dt.ez.kafka.tools.former.ProducerEngine;


public class SimpleKafkaProducer {
	
//	private static String [] datas = {
//		"{\"DATETIME\":\"20170607135832\",\"TYPE\":\"0\",\"YWDW\":\"B5C50A1A-587C-4583-810C-A8D2423D6580-00154\",\"SBBM\":\"0001\",\"JZBM\":\"150500000001\",\"value\":[{\"POINTCODE\":\"150500000001000100000002\",\"DATA\":\"238.1000\",\"GUID\":\"9c878deb-6e58-47db-b1e7-b9e4569cdfec\",\"EVT_STATION\":\"智慧大厦配电室\",\"EVT_INFO\":\"1#变低压出线开关:电压越限（电压偏差越限）\",\"TERM_ID\":\"D7B58FF8-0421-4DD1-8E3C-4F61BA63E681-00341\"}]}",
//		"{\"DATETIME\":\"20170607135352\",\"TYPE\":\"0\",\"YWDW\":\"null\",\"SBBM\":\"0003\",\"JZBM\":\"130602000003\",\"value\":[{\"POINTCODE\":\"130602000003000300000002\",\"DATA\":\"136.6799\",\"GUID\":\"c79c1deb-0f2d-40a9-aa99-9a27770d3f65\",\"EVT_STATION\":\"[未知运维单位]\",\"EVT_INFO\":\"[未知设备]:电压越限（电压偏差越限）\",\"TERM_ID\":\"[未知测点]\"},{\"POINTCODE\":\"130602000003000300000003\",\"DATA\":\"135.8200\",\"GUID\":\"5bb20a45-ed43-4e37-8d70-e7343d757c58\",\"EVT_STATION\":\"[未知运维单位]\",\"EVT_INFO\":\"[未知设备]:电压越限（电压偏差越限）\",\"TERM_ID\":\"[未知测点]\"}]}",
//		"{\"DATETIME\":\"20170607135353\",\"TYPE\":\"0\",\"YWDW\":\"null\",\"SBBM\":\"0002\",\"JZBM\":\"130602000003\",\"value\":[{\"POINTCODE\":\"130602000003000200000007\",\"DATA\":\"42.7000\",\"GUID\":\"1d184e75-ee6f-470c-8e2b-788696e7c24c\",\"EVT_STATION\":\"[未知运维单位]\",\"EVT_INFO\":\"[未知设备]:频率偏差（频率偏差越限）\",\"TERM_ID\":\"[未知测点]\"}]}"
//	};

	/**
	 * 
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> SASL file
	 */
	public static void main (String [] args) {
		String [] datas = {
			"1",
			"2",
			"3"
		};
		
		if (args.length < 3) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <groupid> <SASLfile>.");
		}
		
		Properties prop = new Properties ();
		prop.put ("producer.topic", args [1]);
		prop.put ("producer.bootstrap.servers", args [0]);
		
		prop.put ("acks", "all");
		prop.put ("retries", 0);
		prop.put ("batch.size", 16384);
		prop.put ("linger.ms", 0);
		prop.put ("buffer.memory", 33554432);
		prop.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		/* For SASL configuration*/
		prop.setProperty ("security.protocol", "SASL_PLAINTEXT");
		prop.setProperty ("sasl.mechanism", "PLAIN");
		
		/* SASL JAAS file. */
		System.setProperty ("java.security.auth.login.config", args [2]);
		ProducerEngine engine = new ProducerEngine (prop);
		
		while (true) {
			engine.send (datas);
			sleep (1000);
			System.out.print (".");
		}

	}
	
	private static void sleep (long ms) {
		try {
			Thread.sleep (ms);
		} catch (Exception ex) {
			ex.printStackTrace ();
		}
	}

}
