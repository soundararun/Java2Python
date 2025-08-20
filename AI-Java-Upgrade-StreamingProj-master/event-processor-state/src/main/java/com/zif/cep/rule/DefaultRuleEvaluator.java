package com.zif.cep.rule;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.zif.cep.core.Alert;
import com.zif.cep.core.DateDeserializer;
import com.zif.cep.core.Keyed;
import com.zif.cep.core.Rule;
import com.zif.cep.core.Alert.Builder;

/**
 * Evaluates rules against the incoming events. It manages the consecutive
 * count, alert status and alert expiry using flink state management.
 * 
 * @author Vijay
 *
 */
public class DefaultRuleEvaluator
		extends KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> {
	private static final Logger LOGGER = Logger.getLogger(DefaultRuleEvaluator.class);
	final static String EMPTY_STRING = "";
	final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
	final TimeZone utc = TimeZone.getTimeZone("UTC");
	ScriptEngine engine = null;

	MapStateDescriptor<String, Long> consecutiveCountMapStateDesc = new MapStateDescriptor<>("consecutiveCount",
			String.class, Long.class);
	MapState<String, Long> consecutiveCountMapState;

	MapStateDescriptor<String, Boolean> alertStatusMapStateDesc = new MapStateDescriptor<>("alertStatus", String.class,
			Boolean.class);
	MapState<String, Boolean> positiveAlertMapState;

	// broadcast state descriptor
	MapStateDescriptor<String, Rule> ruleDesc;

	@Override
	public void open(Configuration conf) {
		LOGGER.debug("\n\nDefaultRuleEvaluator - open called");
		dateFormat.setTimeZone(utc);
		ScriptEngineManager engineManager = new ScriptEngineManager();
		engine = engineManager.getEngineByName("JavaScript");

		consecutiveCountMapState = getRuntimeContext().getMapState(consecutiveCountMapStateDesc);
		positiveAlertMapState = getRuntimeContext().getMapState(alertStatusMapStateDesc);
		ruleDesc = new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO,
				TypeInformation.of(new TypeHint<Rule>() {
				}));
	}

	/**
	 * Called for each user event. Evaluates the current rule against the previous
	 * and current event.
	 */
	@Override
	public void processElement(Keyed<JSONObject, String, String> keyed, ReadOnlyContext ctx, Collector<Alert> out) {
		LOGGER.debug("\n\nDefaultRuleEvaluator - processElement called with keyed -> " + keyed.toString());
		try {
			JSONObject jsonObj = keyed.getWrapped();
			String machineName = (String) jsonObj.get("machine_name");
			String eventTimestamp = (String) jsonObj.get("timestamp");
			for (Entry<String, Rule> entry : ctx.getBroadcastState(ruleDesc).immutableEntries()) {
				Rule rule = entry.getValue();
				if ("INACTIVE".equals(rule.getStatus()))
					continue;
				String stateKey = getStateKey(rule.getRuleId(), machineName, rule.getResourceType());
				String alertKey = getAlertKey(stateKey, dateFormat.parse(eventTimestamp));
				boolean isRuleMatch = evaluateRuleExpr(rule, jsonObj);
				/*
				 * If Rule Matches, there could be 2 possibilities. Either it is a POSITIVE
				 * alert or just a plain WARNING
				 * 
				 * If currentConsecutiveCount matches consecutiveLimit, then promote the warning
				 * to a POSITIVE alert
				 * 
				 * Else if Rule doesn't match, check if the positive alert is already raised and
				 * the current consecutive count has exceeded the consecutive limit, then simply
				 * raise REVERSE alert and clear the state
				 * 
				 */
				if (isRuleMatch) {
					initializeStateIfAbsent(stateKey);
					long currConsecutiveCount = incrementConsecutiveCount(stateKey);
					if (rule.getConsecutiveLimit().equals(currConsecutiveCount)
							&& positiveAlertMapState.get(stateKey) == false) {
						Alert alert = buildAlert(jsonObj, rule, alertKey);
						sendPositiveAlert(alert, out);
						setPositiveAlertStatusInState(stateKey);
					}
				} else if (wasPositiveAlertRaisedAlready(stateKey, rule.getConsecutiveLimit())) {
					Alert alert = buildAlert(jsonObj, rule, alertKey);
					sendReverseAlert(alert, out);
					clearPositiveAlertFromState(stateKey);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - processElement", e);
		}
	}

	long incrementConsecutiveCount(String stateKey) throws Exception {
		Long prevConsecutiveCount = consecutiveCountMapState.get(stateKey);
		Long currConsecutiveCount = prevConsecutiveCount + 1;
		setCurrentConsecutiveCountInState(stateKey, currConsecutiveCount);
		return currConsecutiveCount;
	}

	/**
	 * Initialize the state for consecutive count(to zero) and positive alert
	 * status(to false) if it doesn't exist
	 * 
	 * @param stateKey
	 */
	private void initializeStateIfAbsent(String stateKey) {
		try {
			if (!consecutiveCountMapState.contains(stateKey) && !positiveAlertMapState.contains(stateKey)) {
				consecutiveCountMapState.put(stateKey, 0L);
				positiveAlertMapState.put(stateKey, false);
			}
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - initializeIfAbsent", e);
		}
	}

	private boolean wasPositiveAlertRaisedAlready(String stateKey, long consecutiveLimit) {
		boolean wasPositiveAlert = false;
		try {
			if (consecutiveCountMapState.contains(stateKey) && getPrevConsecutiveCount(stateKey) >= consecutiveLimit
					&& positiveAlertMapState.contains(stateKey) && positiveAlertMapState.get(stateKey) == true) {
				wasPositiveAlert = true;
			}
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - initializeIfAbsent", e);
		}
		return wasPositiveAlert;
	}

	long getPrevConsecutiveCount(String stateKey) throws Exception {
		return consecutiveCountMapState.get(stateKey);
	}

	private boolean evaluateRuleExpr(Rule rule, JSONObject jsonObj) {
		boolean isRuleMatch = false;
		if (isRuleRelevant(rule, jsonObj)) {
			String ruleExpr = rule.getRuleExpr();
			Set<String> fieldNames = getMatchingFields(ruleExpr, jsonObj);
			engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();// clear all previous bindings
			Bindings vars = engine.getBindings(ScriptContext.ENGINE_SCOPE);
			fieldNames.forEach(fieldName -> vars.put(fieldName, jsonObj.get(fieldName)));
			try {
				isRuleMatch = (boolean) engine.eval(ruleExpr);
			} catch (ScriptException e) {
				LOGGER.error("Unexpected error in DefaultRuleEvaluator - evaluateRuleExpr", e);
			}
		}
		return isRuleMatch;
	}

	private boolean isRuleRelevant(Rule rule, JSONObject jsonObj) {
		String eventToolName = (String) jsonObj.get("tool_name");
		String eventGroupName = (String) jsonObj.get("group_name");
		String eventMachineName = (String) jsonObj.get("machine_name");
		String eventResourceType = (String) jsonObj.get("resource_type");
		boolean isRelevant = true;
		if (rule != null && jsonObj != null) {
			if (eventToolName != null && eventToolName.equals(rule.getToolName()) && eventResourceType != null
					&& rule.getResourceType().equals(eventResourceType)) {
				if (rule.getGroupNames() != null && rule.getGroupNames().size() > 0
						&& !rule.getGroupNames().contains(eventGroupName)) { // false
					isRelevant = false;
				}
				if ((rule.getDeviceNames() != null && rule.getDeviceNames().size() > 0
						&& !rule.getDeviceNames().contains(eventMachineName))) {// device name null
					isRelevant = false;
				}
			} else {
				isRelevant = false;
			}
		} else {
			isRelevant = false;
		}
		return isRelevant;
	}

	private String getActualValue() {
		Bindings vars = engine.getBindings(ScriptContext.ENGINE_SCOPE);
		String actualValue = vars.keySet().stream().map(key -> key + "=" + vars.get(key))
				.collect(Collectors.joining(", ", "{", "}"));
		return actualValue;
	}

	private Alert buildAlert(JSONObject jsonObj, Rule rule, String alertKey) {
		String machineName = (String) jsonObj.get("machine_name");
		String eventTimestamp = (String) jsonObj.get("timestamp");
		String resourceType = (String) jsonObj.get("resource_type");
		String group = (String) jsonObj.get("group_name");
		String actualValue = getActualValue();
		String others = getOthers(jsonObj);
		String alertDescription = getAlertDescription(jsonObj, rule, actualValue);
		String counterName = EMPTY_STRING;
		String gtmCaseId = null;// Null
		String gtmStatus = null;

		Alert alert = null;
		try {
			alert = new Alert.Builder().setAlertKey(alertKey).setMachineId(machineName).setResourceType(resourceType)
					.setGroup(group).setActualValue(actualValue)
					.setEventReceivedTimestamp(dateFormat.parse(eventTimestamp)).setAlertGeneratedTimestamp(new Date())
					.setAlertType("WARNING").setToolName(rule.getToolName()).setDescription(alertDescription)
					.setOthers(others).setCounterName(counterName).setGtmCaseId(gtmCaseId).setGtmStatus(gtmStatus)
					.build();
		} catch (ParseException e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - buildAlert", e);
		}
		return alert;
	}

	private String getAlertDescription(JSONObject jsonObj, Rule rule, String actualValue) {
		StringBuffer alertDescription = new StringBuffer();
		alertDescription.append(rule.getRuleDescription());
		if (jsonObj.containsKey("alert_message") && jsonObj.get("alert_message") != null) {
			String alertMessage = (String) jsonObj.get("alert_message");
			alertDescription.append("|").append(alertMessage);
		}
		alertDescription.append("|").append(actualValue);
		return alertDescription.toString();
	}

	private String getOthers(JSONObject jsonObj) {
		String others = EMPTY_STRING;
		if (jsonObj.containsKey("others") && jsonObj.get("others") != null) {
			others = (String) jsonObj.get("others");
		}
		return others;
	}

	private String getStateKey(String ruleId, String machineName, String resourceType) {
		String stateKey = "ruleId:" + ruleId + "|machineId:" + machineName + "|eventType:" + resourceType;
		return stateKey;
	}

	private String getAlertKey(String stateKey, Date eventgeneratedtime) {
		String alertKey = stateKey + "|type:cep_alert|" + eventgeneratedtime.getTime();
		return alertKey;
	}

	private void sendPositiveAlert(Alert alert, Collector<Alert> out) {
		LOGGER.debug("\n\nDefaultRuleEvaluator - positiveAlert");
		alert.setAlertType("POSITIVE");
		try {
			out.collect(alert);
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - positiveAlert", e);
		}
	}

	private void sendReverseAlert(Alert alert, Collector<Alert> out) {
		LOGGER.debug("\n\nDefaultRuleEvaluator - reverseAlert");
		try {
			alert.setAlertType("REVERSE");
			alert.setConsecutiveCount(0L);
			out.collect(alert);
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - reverseAlert", e);
		}
	}

	void setCurrentConsecutiveCountInState(String stateKey, Long currConsecutiveCount) throws Exception {
		consecutiveCountMapState.put(stateKey, currConsecutiveCount);
	}

	void setPositiveAlertStatusInState(String stateKey) throws Exception {
		positiveAlertMapState.put(stateKey, true);
	}

	void clearPositiveAlertFromState(String stateKey) throws Exception {
		consecutiveCountMapState.remove(stateKey);
		positiveAlertMapState.remove(stateKey);
	}

	private Set<String> getMatchingFields(String ruleExpr, JSONObject data) {
		String[] tokens = ruleExpr.split(" ");
		List<String> tokenList = Arrays.asList(tokens);
		Set<String> keys = data.keySet();
		Set<String> fieldNames = keys.stream().filter(tokenList::contains).collect(Collectors.toSet());
		return fieldNames;
	}

	/**
	 * Called for each new rule. Overwrites the current rule with the new rule.
	 */
	@Override
	public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out) {
		LOGGER.debug("\n\nDefaultRuleEvaluator - processBroadcastElement called with rule -> " + rule.toString());
		// store the new rule by updating the broadcast state
		BroadcastState<String, Rule> bcState = ctx.getBroadcastState(ruleDesc);
		try {
			bcState.put(rule.getRuleId(), rule);
		} catch (Exception e) {
			LOGGER.error("Unexpected error in DefaultRuleEvaluator - processBroadcastElement", e);
		}
	}
}
