package com.zif.cep.core;

import static com.zif.cep.core.ProcessingUtils.handleRuleBroadcast;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import com.zif.cep.core.StreamingJob.Descriptors;

public class DynamicKeyFunction extends BroadcastProcessFunction<JSONObject, Rule, Keyed<JSONObject, String, String>> {
	private static final Logger LOGGER = Logger.getLogger(DynamicKeyFunction.class);
	Map<String, String> partitionMap;
	List<String> partitionKeyList;

	public DynamicKeyFunction(String partitionKey) {		
		this.partitionKeyList = Arrays.asList(partitionKey.split(","));
		this.partitionMap = loadPartitionMap(partitionKey);
	}

	private Map<String, String> loadPartitionMap(String partitionKey) {
		List<String> partitionKeyList = Arrays.asList(partitionKey.split(","));
		Map<String, String> partitionMap = new HashMap<String, String>();
		Field[] fields = Rule.class.getDeclaredFields();
		for (Field field : fields) {
			if (field.isAnnotationPresent(JsonProperty.class)) {
				String annotationValue = field.getAnnotation(JsonProperty.class).value();
				if (partitionKeyList.contains(annotationValue))
					partitionMap.put(annotationValue, field.getName());
			}
		}
		return partitionMap;
	}

	@Override
	public void open(Configuration parameters) {
		LOGGER.debug("\n\nDynamicKeyFunction - open called");
	}

	@Override
	public void processElement(JSONObject jsonObj, ReadOnlyContext ctx,
			Collector<Keyed<JSONObject, String, String>> out) {
		if (jsonObj != null) {
			LOGGER.debug("\n\nDynamicKeyFunction - processElement called with jsonObj -> " + jsonObj.toString());
			ReadOnlyBroadcastState<String, Rule> rulesState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
			forkEventForEachGroupingKey(jsonObj, rulesState, out);
		}
	}

	private void forkEventForEachGroupingKey(JSONObject jsonObj, ReadOnlyBroadcastState<String, Rule> rulesState,
			Collector<Keyed<JSONObject, String, String>> out) {
		try {
			for (Map.Entry<String, Rule> entry : rulesState.immutableEntries()) {

				final Rule rule = entry.getValue();
				boolean isMatch = partitionMap.entrySet().stream().allMatch(partitionEntry -> {
					final String jsonPropName = partitionEntry.getKey();
					final String fieldName = partitionEntry.getValue();
					final String eventValue = (String) jsonObj.get(jsonPropName);
					try {
						final Object ruleValueObj = Rule.class.getField(fieldName).get(rule);
						// groupNames and deviceNames are of type List
						if (fieldName.equals("groupNames") || fieldName.equals("deviceNames")) {
							final List<String> ruleValues = (List<String>) ruleValueObj;
							if (ruleValues != null && ruleValues.size() > 0 && eventValue != null) {
								return ruleValues.contains(eventValue);
							}
						} // Other fields are of type String
						else {
							final String ruleValue = (String) ruleValueObj;
							if (ruleValue != null && eventValue != null) {
								return ruleValue.equals(eventValue);
							}
						}
					} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
							| SecurityException e) {
						LOGGER.error("Unexpected error in DynamicKeyFunction - forkEventForEachGroupingKey", e);
					}
					return false;
				});
				if (isMatch) {
					out.collect(new Keyed<>(jsonObj, JsonKeysExtractor.getKey(partitionKeyList, jsonObj),
							rule.getRuleId()));
				}
			}
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DynamicKeyFunction - forkEventForEachGroupingKey", e);
		}
	}

	@Override
	public void processBroadcastElement(Rule rule, Context ctx, Collector<Keyed<JSONObject, String, String>> out) {
		LOGGER.debug("\n\nDynamicKeyFunction - processBroadcastElement called with rule -> " + rule.toString());
		BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
		try {
			handleRuleBroadcast(rule, broadcastState);
			/*
			 * if (rule.getRuleState() == RuleState.CONTROL) {
			 * handleControlCommand(rule.getControlType(), broadcastState); }
			 */
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DynamicKeyFunction - processBroadcastElement", e);
		}

	}

	/*
	 * private void handleControlCommand(ControlType controlType,
	 * BroadcastState<String, Rule> rulesState) throws Exception { switch
	 * (controlType) { case DELETE_RULES_ALL: Iterator<Entry<String, Rule>>
	 * entriesIterator = rulesState.iterator(); while (entriesIterator.hasNext()) {
	 * Entry<String, Rule> ruleEntry = entriesIterator.next();
	 * rulesState.remove(ruleEntry.getKey()); } break; } }
	 */
}
