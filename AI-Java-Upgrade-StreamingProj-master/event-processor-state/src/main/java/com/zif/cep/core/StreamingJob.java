package com.zif.cep.core;

import static com.zif.cep.config.Parameters.BOOL_PARAMS;
import static com.zif.cep.config.Parameters.INT_PARAMS;
import static com.zif.cep.config.Parameters.STRING_PARAMS;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.zif.cep.config.Config;
import com.zif.cep.config.Parameters;

/*
 * TODO
 * 1.Bug on Reschedule timer. Store map with Alert key instead of ruleId
 * 2.Logger statements, Exception Handling & Comments
 * 3.Zookeeper integration for High availability.Check
 * 4.Performance Module - Add Group(MC) and present time and remove encryption
 * 5.Watermark check
 * 
 */

/**
 * This job pipeline has the following features. A) CONFIG - Ability to pick
 * custom configuration. In absence of it, it falls back to default
 * configuration. B) CHECKPOINTING - Optional feature. C) PARTITIONING - Ability
 * to partition keys dynamically based on user defined rules. D) CUSTOM
 * FUNCTIONS - There are 2 modules for Rule Evaluation and Alert Generation.
 * Option to choose between custom and default implementation.
 * 
 * This class performs the following steps.
 * 
 * It loads the configuration parameters from default/overridden settings
 * 
 * Based on configuration applies checkpoints
 * 
 * Loads the event stream from kafka of type JSONObject
 * 
 * Loads the broadcasted rule stream from kafka of type Rule
 * 
 * Partitions the incoming events based on the partition keys from Rules and
 * generate a Keyed stream of type Keyed<JSONObject, String, String>
 * 
 * Loads the default/overridden rule evaluator using reflection and invokes the
 * lifecycle methods. The outcome of rule evaluation is collected as Alerts
 *
 * 
 * 
 * @author Vijay, Monisha
 *
 */
public class StreamingJob {
	private static final Logger LOGGER = Logger.getLogger(StreamingJob.class);

	SourceFunction<JSONObject> eventSource;
	SourceFunction<Rule> ruleSource;
	SinkFunction<Alert> alertSink;
	KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> ruleEvaluatorFunction;
	Config config;

	public StreamingJob(SourceFunction<JSONObject> eventSource, SourceFunction<Rule> ruleSource,
			SinkFunction<Alert> alertSink,
			KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> ruleEvaluatorFunction,
			Config config) {
		this.eventSource = eventSource;
		this.ruleSource = ruleSource;
		this.alertSink = alertSink;
		this.ruleEvaluatorFunction = ruleEvaluatorFunction;
		this.config = config;
	}

	public static void main(String[] args) {
		LOGGER.debug("Inside StreamingJob - main method");
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setBufferTimeout(5);// To minimize latency, set the timeout to a value close to 0 (for example 5/10
									// ms)
			Config config = loadConfig(args);
			applyCheckpoints(env, config);
			SourceFunction<Rule> ruleSource = loadRuleSource(config);
			SourceFunction<JSONObject> eventSource = loadEventSource(config);
			SinkFunction<Alert> alertSink = loadAlertSink(config);
			KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> ruleEvaluatorFunction = loadRuleEvaluator(
					config);
			new StreamingJob(eventSource, ruleSource, alertSink, ruleEvaluatorFunction, config).build(env);
			env.setParallelism(config.get(Parameters.PARALLELISM_FACTOR));
			env.execute();
		} catch (Exception e) {
			LOGGER.error("StreamingJob exception", e);
		}
	}

	public void build(StreamExecutionEnvironment env) {
		BroadcastStream<Rule> ruleBroadcastStream = fetchBroadcastStreamFromRuleSource(env);
		DataStream<JSONObject> eventStream = fetchStreamFromEventSource(env);

		DataStream<Keyed<JSONObject, String, String>> keyedEventStream = eventStream.connect(ruleBroadcastStream)
				.process(new DynamicKeyFunction(config.get(Parameters.PARTITION_KEY))).uid("DynamicKeyFunction")
				.name("Dynamic Partitioning Function").keyBy((keyed) -> keyed.getKey());

		DataStream<Alert> alertStream = keyedEventStream.connect(ruleBroadcastStream).process(ruleEvaluatorFunction)
				.uid("DynamicAlertFunction").name("Dynamic Rule Evaluation Function");
		postAlertsToSink(alertStream);
	}

	public static Config loadConfig(String[] args) {
		ParameterTool tool = ParameterTool.fromArgs(args);
		Parameters inputParams = new Parameters(tool);
		Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
		return config;
	}

	private static void applyCheckpoints(StreamExecutionEnvironment env, Config config) {
		boolean enableCheckpoints = config.get(Parameters.ENABLE_CHECKPOINTS);
		if (enableCheckpoints) {
			int checkpointsInterval = config.get(Parameters.CHECKPOINT_INTERVAL);
			int minPauseBtwnCheckpoints = config.get(Parameters.CHECKPOINT_INTERVAL);
			env.enableCheckpointing(checkpointsInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
		}
	}

	private static FlinkKafkaConsumer011<JSONObject> loadEventSource(Config config) {
		FlinkKafkaConsumer011<JSONObject> eventKafkaConsumer = Consumers.createEventConsumer(config);
		eventKafkaConsumer.setStartFromEarliest();
		// eventKafkaConsumer.setStartFromGroupOffsets(); // the default behaviour
		return eventKafkaConsumer;
	}

	private DataStream<JSONObject> fetchStreamFromEventSource(StreamExecutionEnvironment env) {
		DataStream<JSONObject> eventInputStream = env.addSource(eventSource);
		return eventInputStream;
	}

	private static FlinkKafkaConsumer011<Rule> loadRuleSource(Config config) {
		FlinkKafkaConsumer011<Rule> ruleKafkaConsumer = Consumers.createRuleConsumer(config);
		ruleKafkaConsumer.setStartFromEarliest();
		// ruleKafkaConsumer.setStartFromGroupOffsets(); // the default behaviour
		return ruleKafkaConsumer;
	}

	private BroadcastStream<Rule> fetchBroadcastStreamFromRuleSource(StreamExecutionEnvironment env) {
		DataStream<Rule> ruleInputStream = env.addSource(ruleSource);
		BroadcastStream<Rule> ruleBroadcastStream = ruleInputStream.broadcast(Descriptors.rulesDescriptor);
		return ruleBroadcastStream;
	}

	private static FlinkKafkaProducer011<Alert> loadAlertSink(Config config) {
		FlinkKafkaProducer011<Alert> alertsProducer = Producers.createWarningProducer(config);
		return alertsProducer;
	}

	private void postAlertsToSink(DataStream<Alert> alertStream) {
		alertStream.addSink(alertSink);
	}

	private static KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> loadRuleEvaluator(
			Config config) {
		String ruleClassName = config.get(Parameters.RULE_MODULE);
		KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> ruleEvaluatorImpl = null;
		try {
			Class<?> clazz = Class.forName(ruleClassName);
			Constructor<?> constructor = clazz.getConstructor();
			Object object = constructor.newInstance();
			if (object instanceof KeyedBroadcastProcessFunction<?, ?, ?, ?>) {
				ruleEvaluatorImpl = (KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert>) object;
			}
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			LOGGER.error("Unexpected error in Main - loadRuleEvaluator", e);
		}
		return ruleEvaluatorImpl;
	}

	public static class Descriptors {
		// a map descriptor to store the name of the rule (string) and the rule itself.
		public static final MapStateDescriptor<String, Rule> rulesDescriptor = new MapStateDescriptor<>(
				"RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Rule>() {
				}));
	}
}
