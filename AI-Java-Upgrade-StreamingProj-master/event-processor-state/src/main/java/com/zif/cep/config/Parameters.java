package com.zif.cep.config;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.java.utils.ParameterTool;

public class Parameters {

	// Rule Evaluator and Alert Generator Modules
	public static final Param<String> RULE_MODULE = Param.string("rule-module",
			"com.zif.cep.rule.DefaultRuleEvaluator");
	public static final Param<String> ALERT_MODULE = Param.string("alert-module",
			"com.zif.cep.alert.DefaultAlertGenerator");

	// Kafka
	public static final Param<String> KAFKA_HOST = Param.string("kafka-host", "localhost");
	public static final Param<Integer> KAFKA_PORT = Param.integer("kafka-port", 9092);
	public static final Param<String> EVENTS_TOPIC = Param.string("data-topic", "events");
	public static final Param<String> RULES_TOPIC = Param.string("rules-topic", "rules");
	public static final Param<String> WARNINGS_TOPIC = Param.string("warnings-topic", "alerts");// warnings
	public static final Param<String> ALERTS_TOPIC = Param.string("alerts-topic", "alerts");
	public static final Param<String> PARTITION_KEY = Param.string("partition-key", "group_name,resource_type");

	// Checkpoint
	public static final Param<Boolean> ENABLE_CHECKPOINTS = Param.bool("checkpoints", true);
	public static final Param<Integer> CHECKPOINT_INTERVAL = Param.integer("checkpoint-interval", 60000);
	public static final Param<Integer> MIN_PAUSE_BETWEEN_CHECKPOINTS = Param.integer("min-pause-btwn-checkpoints",
			10000);
	public static final Param<Integer> PARALLELISM_FACTOR = Param.integer("parallelism-factor", 1);

	public static final List<Param<String>> STRING_PARAMS = Arrays.asList(KAFKA_HOST, EVENTS_TOPIC, WARNINGS_TOPIC,
			ALERTS_TOPIC, RULES_TOPIC, RULE_MODULE, ALERT_MODULE, PARTITION_KEY);
	public static final List<Param<Integer>> INT_PARAMS = Arrays.asList(KAFKA_PORT, CHECKPOINT_INTERVAL,
			MIN_PAUSE_BETWEEN_CHECKPOINTS, PARALLELISM_FACTOR);
	public static final List<Param<Boolean>> BOOL_PARAMS = Arrays.asList(ENABLE_CHECKPOINTS);

	private final ParameterTool tool;

	public Parameters(ParameterTool tool) {
		this.tool = tool;
	}

	<T> T getOrDefault(Param<T> param) {
		if (!tool.has(param.getName())) {
			return param.getDefaultValue();
		}
		Object value;
		if (param.getType() == Integer.class) {
			value = tool.getInt(param.getName());
		} else if (param.getType() == Long.class) {
			value = tool.getLong(param.getName());
		} else if (param.getType() == Double.class) {
			value = tool.getDouble(param.getName());
		} else if (param.getType() == Boolean.class) {
			value = tool.getBoolean(param.getName());
		} else {
			value = tool.get(param.getName());
		}
		return param.getType().cast(value);
	}

	public static Parameters fromArgs(String[] args) {
		ParameterTool tool = ParameterTool.fromArgs(args);
		return new Parameters(tool);
	}
}