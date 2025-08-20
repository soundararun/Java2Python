package com.zif.cep.test;

import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONObject;

import com.zif.cep.core.Rule;

public class TestDataGenerator {	
		
	static Rule fetchCpuRule() {

		Rule cpuRule = new Rule.Builder().setRuleId("1").setRuleName("High CPU utilization")
				.setRuleDescription("High CPU utilization").setRuleMessage("cpu_time_idle is exceeded")
				.setToolName("SolarWinds").setGroupNames(Arrays.asList("MC", "BE"))
				.setDeviceNames(Arrays.asList("Centos1", "Centos2")).setRuleExpr("cpu_time_idle >= 90")
				.setResourceType("cpu").setPriority("").setConsecutiveLimit(3L).setStatus("ACTIVE")
				.build();
		return cpuRule;
	}	

	static JSONObject fetchRuleMatchCpuEvent() {
		JSONObject cpuEvent = new JSONObject();
		cpuEvent.put("name", "InfraData");
		cpuEvent.put("agent_version", "1.0");
		cpuEvent.put("tool_name", "SolarWinds");
		cpuEvent.put("group_name", "MC");
		cpuEvent.put("machine_name", "Centos1");
		cpuEvent.put("ip_address", "172.18.0.5");
		cpuEvent.put("os", "Linux");
		cpuEvent.put("timestamp", "2020-04-21T18:42:41Z");
		cpuEvent.put("resource_type", "cpu");
		cpuEvent.put("cpu_count", 4);
		cpuEvent.put("cpu_time_idle", 98.04);
		cpuEvent.put("processor_processor_time", 2.2719017);
		return cpuEvent;
	}

	static JSONObject fetchRuleMismatchCpuEvent() {
		JSONObject cpuEvent = new JSONObject();
		cpuEvent.put("name", "InfraData");
		cpuEvent.put("agent_version", "1.0");
		cpuEvent.put("tool_name", "SolarWinds");
		cpuEvent.put("group_name", "MC");
		cpuEvent.put("machine_name", "Centos1");
		cpuEvent.put("ip_address", "172.18.0.5");
		cpuEvent.put("os", "Linux");
		cpuEvent.put("timestamp", "2020-04-21T18:42:41Z");
		cpuEvent.put("resource_type", "cpu");
		cpuEvent.put("cpu_count", 4);
		cpuEvent.put("cpu_time_idle", 70.04);
		cpuEvent.put("processor_processor_time", 2.2719017);
		return cpuEvent;
	}
	
	static Rule fetchSyslogRule() {

		Rule syslogRule = new Rule.Builder().setRuleId("2").setRuleName("syslog rule")
				.setRuleDescription("test syslog body").setRuleMessage("test syslog")
				.setToolName("Universal Connector").setGroupNames(Arrays.asList("Syslog"))
				.setDeviceNames(Arrays.asList("zif4vm2")).setRuleExpr("new RegExp('No living connections','g').test( body )")
				.setResourceType("syslog").setPriority("MAJOR").setConsecutiveLimit(3L).setStatus("ACTIVE")
				.build();
		return syslogRule;
	}
	
	
	static JSONObject fetchRuleMatchSyslogEvent() {
		JSONObject syslogEvent = new JSONObject();
		syslogEvent.put("name", "syslog data");
		syslogEvent.put("tool_name", "Universal Connector");
		syslogEvent.put("group_name", "Syslog");
		syslogEvent.put("machine_name", "zif4vm2");
		syslogEvent.put("timestamp", "2020-11-09T07:45:21Z");
		syslogEvent.put("resource_type", "syslog");
		syslogEvent.put("priority", "30");
		syslogEvent.put("severity", "6");
		syslogEvent.put("facility", "3");
		syslogEvent.put("sender", "/10.1.0.5");
		syslogEvent.put("body", "kibana: {\\\"type\\\":\\\"log\\\",\\\"@timestamp\\\":\\\"2020-11-09T07:45:21Z\\\",\\\"tags\\\":[\\\"warning\\\",\\\"elasticsearch\\\",\\\"data\\\"],\\\"pid\\\":591,\\\"message\\\":\\\"No living connections\\\"}");
		syslogEvent.put("protocol", "TCP");
		syslogEvent.put("port", "514");
		return syslogEvent;
	}

	static JSONObject fetchRuleMismatchSyslogEvent() {
		JSONObject syslogEvent = new JSONObject();
		syslogEvent.put("name", "syslog data");
		syslogEvent.put("tool_name", "Universal Connector");
		syslogEvent.put("group_name", "Syslog");
		syslogEvent.put("machine_name", "zif4vm2");
		syslogEvent.put("timestamp", "2020-11-09T07:45:21Z");
		syslogEvent.put("resource_type", "syslog");
		syslogEvent.put("priority", "30");
		syslogEvent.put("severity", "6");
		syslogEvent.put("facility", "3");
		syslogEvent.put("sender", "/10.1.0.5");
		syslogEvent.put("body", "kibana: {\\\"type\\\":\\\"log\\\",\\\"@timestamp\\\":\\\"2020-11-09T07:45:21Z\\\",\\\"tags\\\":[\\\"warning\\\",\\\"elasticsearch\\\",\\\"data\\\"],\\\"pid\\\":591,\\\"message\\\":\\\"Alive connections\\\"}");
		syslogEvent.put("protocol", "TCP");
		syslogEvent.put("port", "514");
		return syslogEvent;
	}
		
	static Rule fetchSnmpRule() {

		Rule syslogRule = new Rule.Builder().setRuleId("3").setRuleName("snmp rule")
				.setRuleDescription("test snmp description").setRuleMessage("test snmp")
				.setToolName("Universal Connector").setGroupNames(Arrays.asList("SNMP"))
				.setDeviceNames(Arrays.asList("FW_ZA1_LOCKBOX")).setRuleExpr("new RegExp('alarmhasbeengenerated','g').test( description ) && new RegExp('NOGROUP','g').test( others )")
				.setResourceType("snmptrap").setPriority("MAJOR").setConsecutiveLimit(3L).setStatus("ACTIVE")
				.build();
		return syslogRule;
	}
	
	
	static JSONObject fetchRuleMatchSnmpEvent() {
		JSONObject snmpEvent = new JSONObject();
		snmpEvent.put("name", "snmptrap data");
		snmpEvent.put("tool_name", "Universal Connector");
		snmpEvent.put("group_name", "SNMP");
		snmpEvent.put("machine_name", "FW_ZA1_LOCKBOX");
		snmpEvent.put("timestamp", "2020-04-21T18:42:41Z");
		snmpEvent.put("resource_type", "snmptrap");
		snmpEvent.put("description", "ACAWAAE(instance:PRDmachine:PRD)alarmhasbeengenerated.");
		snmpEvent.put("status", "JOBFAILURE");
		snmpEvent.put("others", "503|1|PRD|NOBOX|PRD|NOGROUP|67327899|1|PRD");
		snmpEvent.put("peer_address", "152.72.195.219/50815");
		snmpEvent.put("syntax", 6);
		return snmpEvent;
	}
	
	static JSONObject fetchRuleMismatchSnmpEvent() {
		JSONObject snmpEvent = new JSONObject();
		snmpEvent.put("name", "snmptrap data");
		snmpEvent.put("tool_name", "Universal Connector");
		snmpEvent.put("group_name", "SNMP");
		snmpEvent.put("machine_name", "FW_ZA1_LOCKBOX");
		snmpEvent.put("timestamp", "2020-04-21T18:42:41Z");
		snmpEvent.put("resource_type", "snmptrap");
		snmpEvent.put("description", "ACAWAAE(instance:PRDmachine:PRD)alarmhasbeengenerated.");
		snmpEvent.put("status", "JOBFAILURE");
		snmpEvent.put("others", "503|1|PRD|NOBOX|PRD|YESGROUP|67327899|1|PRD");
		snmpEvent.put("peer_address", "152.72.195.219/50815");
		snmpEvent.put("syntax", 6);
		return snmpEvent;
	}

}
