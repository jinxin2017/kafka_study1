package com.study.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class ProducerDemo {
	private static final String BROKER_LIST = "kafka0:9092,kafka1:9093,kafka2:9094";
	private static final String TOPIC = "topic1";

	public static void main(String[] args) throws InterruptedException {
		Producer<String, String> producer = initProducer();
		sendMes(producer, TOPIC);
	}

	private static Producer<String, String> initProducer()
			throws InterruptedException {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", BROKER_LIST);
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("partitioner.class", RandomPartitioner.class.getName());
		properties.put("producer.type", "sync");
		properties.put("batch.num.messages", "1");
		properties.put("queue.buffer.max.ms", "10000000");
		properties.put("queue.buffering.max.messages", "1000000");
		properties.put("queue.enqueue.timeout.ms", "2000");
//		properties.put("request.required.acks", "-1");
		
		ProducerConfig config = new ProducerConfig(properties);
		Producer<String, String> producer = new Producer<String, String>(config);
		return producer;
	}

	public static void sendMes(Producer<String, String> producer, String topic) {
		try {
			KeyedMessage<String, String> message1 = new KeyedMessage<String, String>(
					topic, "31", "test 31");
			producer.send(message1);
			KeyedMessage<String, String> message2 = new KeyedMessage<String, String>(
					topic, "32", "test 32");
			producer.send(message2);
			KeyedMessage<String, String> message3 = new KeyedMessage<String, String>(
					topic, "33", "test 33");
			producer.send(message3);
			KeyedMessage<String, String> message4 = new KeyedMessage<String, String>(
					topic, "34", "test 34");
			producer.send(message4);
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
