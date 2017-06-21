package com.joinbright.alarmer.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka生产者数据引擎
 * 
 * @author 柴诗雨
 *
 */
public class ProducerEngine {
	private String topic;
	private Producer<String, String> producer;

	/**
	 * 根据Properties文件初始化ProducerEngine，主要获取Kafka Server地址和topic
	 * 
	 * @param props
	 */
	public ProducerEngine(Properties props) {
		this.topic = props.getProperty("producer.topic", "warning");
		// props.getProperty("producer.bootstrap.servers", "M1:9092")
		String bootstrap = props.getProperty ("producer.bootstrap.servers", "localhost:9092");
		props.setProperty ("bootstrap.servers", bootstrap);
		this.producer = createProducer (props);
	}

	/**
	 * 发送消息到Kafka服务器
	 * 
	 * @param messages
	 *            KafKa消息
	 */
	public void send(String... messages) {
		for (int i = 0, n = messages.length; i < n; i++)
			producer.send(new ProducerRecord<String, String>(topic, messages[i]));
	}

	/**
	 * 根据服务器地址和端口号创建Kafka生产者
	 * 
	 * @param server
	 *            ip地址+端口号形式，比如192.168.1.100:9092，一般Kafka服务接口都为9092
	 * @return
	 */
	private Producer<String, String> createProducer (String server) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", server);
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 0);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = createProducer (properties);
		return producer;
	}
	
	private Producer <String, String> createProducer (Properties props) {		
		Producer<String, String> producer = new KafkaProducer<> (props);
		return producer;
	}
	
}
