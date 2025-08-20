package com.zif.cep.core;

import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import java.util.Arrays;
import java.util.List;


import com.zif.cep.config.Config;
import com.zif.cep.config.Parameters;
import java.util.ArrayList;

public class Consumers {
	public static final String CONSUMER_GROUP = "zif";	

	public static FlinkKafkaConsumer011<Rule> createRuleConsumer(Config config) {
		String ruleTopic = config.get(Parameters.RULES_TOPIC);
		FlinkKafkaConsumer011<Rule> consumer = new FlinkKafkaConsumer011<Rule>(ruleTopic, new RuleDeserializationSchema(),
				getKafkaProperties(config));
		return consumer;
	}

	public static FlinkKafkaConsumer011<JSONObject> createEventConsumer(Config config) {
		String topics = config.get(Parameters.EVENTS_TOPIC);
		List<String> eventsTopic = new ArrayList<String>(Arrays.asList(topics.split(",")));
		//String[] topicsArr = topics.split(",");
		//List<String> eventsTopic = Arrays.asList(topicsArr);		
		FlinkKafkaConsumer011<JSONObject> consumer = new FlinkKafkaConsumer011<>(eventsTopic,
				new JSONObjectDeserializationSchema(), getKafkaProperties(config));
		return consumer;
	}
	
	private static Properties getKafkaProperties(Config config) {
		String kafkaHost = config.get(Parameters.KAFKA_HOST);
		Integer kafkaPort = config.get(Parameters.KAFKA_PORT);
		String kafkaAddress = kafkaHost + ":" + kafkaPort;
		String strSerializer = StringSerializer.class.getName();
		String longSerializer = LongSerializer.class.getName();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaAddress);
		properties.setProperty("group.id", CONSUMER_GROUP);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, longSerializer);
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, strSerializer);
		properties.setProperty("acks", "0");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("max.request.size", 15728640);// 15MB
		return properties;
	}
}
