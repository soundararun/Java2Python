package com.zif.cep.core;

import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.zif.cep.config.Config;
import com.zif.cep.config.Parameters;

public class Producers {
    public static FlinkKafkaProducer011<Alert> createWarningProducer(Config config) {
    	String warningTopic = config.get(Parameters.WARNINGS_TOPIC);
    	String kafkaHost = config.get(Parameters.KAFKA_HOST);
		Integer kafkaPort = config.get(Parameters.KAFKA_PORT);
		String kafkaAddress = kafkaHost + ":" + kafkaPort;
        //return new FlinkKafkaProducer011<Alert>(kafkaAddress, warningTopic, new AlertSerializationSchema());
		return new FlinkKafkaProducer011<Alert>(warningTopic, new AlertSerializationSchema(), getProducerProps(kafkaAddress));
    }
    
    public static FlinkKafkaProducer011<Alert> createAlertProducer(Config config) {
    	String alertTopic = config.get(Parameters.ALERTS_TOPIC);
    	String kafkaHost = config.get(Parameters.KAFKA_HOST);
		Integer kafkaPort = config.get(Parameters.KAFKA_PORT);
		String kafkaAddress = kafkaHost + ":" + kafkaPort;
        //return new FlinkKafkaProducer011<Alert>(kafkaAddress, alertTopic, new AlertSerializationSchema());
		return new FlinkKafkaProducer011<Alert>(alertTopic, new AlertSerializationSchema(), getProducerProps(kafkaAddress));
    }
    
    private static Properties getProducerProps(String kafkaAddress) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		//String strSerializer = StringSerializer.class.getName();
		//String longSerializer = LongSerializer.class.getName();		
		//props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, longSerializer);
		//props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, strSerializer);
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("max.request.size", 15728640);// 15MB
		return props;
	}  

}
