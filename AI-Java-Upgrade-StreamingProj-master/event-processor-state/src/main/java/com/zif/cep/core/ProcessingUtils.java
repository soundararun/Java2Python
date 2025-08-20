package com.zif.cep.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.log4j.Logger;

class ProcessingUtils {
	private static final Logger LOGGER = Logger.getLogger(ProcessingUtils.class);

	static void handleRuleBroadcast(Rule rule, BroadcastState<String, Rule> broadcastState) throws Exception {
		switch (rule.getStatus()) {
		case "ACTIVE":
		case "PAUSE":
			broadcastState.put(rule.getRuleId(), rule);
			break;
		case "INACTIVE":
			broadcastState.remove(rule.getRuleId());
			break;
		}
	}

	static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value) throws Exception {

		Set<V> valuesSet = mapState.get(key);

		if (valuesSet != null) {
			valuesSet.add(value);
		} else {
			valuesSet = new HashSet<>();
			valuesSet.add(value);
		}
		mapState.put(key, valuesSet);
		return valuesSet;
	}

	static synchronized void appendRuleExprIfAny(Rule rule) throws IOException{
		StringBuilder newRuleExpr = new StringBuilder();
		newRuleExpr.append(rule.getRuleExpr());
		String toolName = rule.getToolName();
		if (toolName != null) {
			//String deviceNamesStrArr = new ObjectMapper().writeValueAsString(deviceNames);
			newRuleExpr.append(" && ").append("tool_name == " + "'" + toolName + "'"); // ppt > 80 && tool_name = "ZIF" && 
		}
		List<String> groupNames = rule.getGroupNames();
		if (groupNames != null && groupNames.size() > 0) {
			String groupNamesStrArr = new ObjectMapper().writeValueAsString(groupNames);
			//newRuleExpr.append(" && `").append(groupNamesStrArr).append(".includes('${ group_name }')`");
			newRuleExpr.append(" && ").append(groupNamesStrArr).append(".indexOf( group_name )>-1");
		}		
		List<String> deviceNames = rule.getDeviceNames();
		if (deviceNames != null && deviceNames.size() > 0) {
			String deviceNamesStrArr = new ObjectMapper().writeValueAsString(deviceNames);
			newRuleExpr.append(" && ").append(deviceNamesStrArr).append(".indexOf( machine_name )>-1");
		}
		//rule.setRuleExpr(newRuleExpr.toString());
		String formattedRuleExpr = newRuleExpr.toString().replaceAll(" AND ", " && ").replaceAll(" OR ", " || ");
		rule.setRuleExpr(formattedRuleExpr);
	}
}
