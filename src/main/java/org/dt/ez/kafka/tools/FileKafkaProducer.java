package org.dt.ez.kafka.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.dt.ez.kafka.tools.former.ProducerEngine;

public class FileKafkaProducer {

	private static String [] datas = new String [0];
	
	private static void initDataFromFile (String fileName) {
		final String encoding = "UTF-8";
		List <String> container = new ArrayList <String> ();
		try {
	        File file = new File (fileName);
	        if (file.isFile () && file.exists ()){ //判断文件是否存在
	            InputStreamReader read = new InputStreamReader (
	            	new FileInputStream (file), encoding);// 考虑到编码格式
	            BufferedReader bufferedReader = new BufferedReader (read);
	            String lineTxt = null;
	            int i = 0; 
	            
	            while ((lineTxt = bufferedReader.readLine ()) != null && i <= 10000){
	            	
	            	String line = lineTxt.trim ();	            	
	        		container.add (line);
	        		++ i;
	            }
	            read.close();
	            datas = container.toArray (datas);
	        }else{
	        	System.out.println("Cannot found file " + fileName);
	        	System.exit (1);
	        }
	    } catch (Exception e) {
	        System.out.println ("Read File error");
	        e.printStackTrace ();
	        System.exit (1);
	    }
	}
	
	/**
	 * 
	 * @param args
	 * 	args [0] -> bootstrap servers
	 *  args [1] -> topics
	 *  args [2] -> data_file
	 *  args [3] -> SASL file
	 */
	public static void main (String [] args) {
		
		if (args.length < 3) {
			System.err.println ("ERROR arguments : <bootstrap_servers> <topics> <data_file> [<SASLfile>].");
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
		
		initDataFromFile (args [2]);
		
		/* For SASL */
		if (args.length > 3) {
			/* For SASL configuration*/
			prop.setProperty ("security.protocol", "SASL_PLAINTEXT");
			prop.setProperty ("sasl.mechanism", "PLAIN");
			
			/* SASL JAAS file. */
			System.setProperty ("java.security.auth.login.config", args [3]);
		}
		ProducerEngine engine = new ProducerEngine (prop);
		
		while (true) {
			engine.send (datas);
			sleep (100);
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
