package com.zif.cep.test;

import java.util.Queue;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.zif.cep.core.Alert;
import com.zif.cep.core.Keyed;
import com.zif.cep.core.Rule;
import com.zif.cep.core.StreamingJob.Descriptors;
import com.zif.cep.rule.DefaultRuleEvaluator;

/**
 * @author Vijay
 *
 */
public class RuleEvaluatorTest {
	private static final Logger LOGGER = Logger.getLogger(RuleEvaluatorTest.class);

	@Test
	public void testDefaultRuleEvaluatorForCpu() {
		DefaultRuleEvaluator defaultRuleEvaluator = new DefaultRuleEvaluator();
		JSONObject matchCpuEventInput = TestDataGenerator.fetchRuleMatchCpuEvent();
		JSONObject mismatchCpuEventInput = TestDataGenerator.fetchRuleMismatchCpuEvent();
		Rule cpuRuleInput = TestDataGenerator.fetchCpuRule();
		String cpuPartitionKey = "{group_name=MC;resource_type=cpu;machine_name=Centos1}";
		String cpuRuleId = "1";
		
		Queue<Object> expectedOutput = applyTestHarness(defaultRuleEvaluator, cpuRuleInput, matchCpuEventInput,
				mismatchCpuEventInput, cpuPartitionKey, cpuRuleId);
		
		Assert.assertEquals(2, expectedOutput.size());

		StreamRecord<Alert> streamRecord1 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert1 = streamRecord1.getValue();
		Assert.assertEquals("Centos1", alert1.getMachineId());
		Assert.assertEquals("cpu", alert1.getResourceType());
		Assert.assertEquals("MC", alert1.getGroup());
		//Assert.assertEquals("1", alert1.getRuleId());
		//Assert.assertEquals("cpu_time_idle >= 90", alert1.getRuleExpr());
		//Assert.assertEquals("cpu_time_idle is exceeded", alert1.getMessage());
		Assert.assertEquals("{cpu_time_idle=98.04}", alert1.getActualValue());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveLimit());
		Assert.assertEquals("POSITIVE", alert1.getAlertType());
		Assert.assertEquals("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert|"
				+ alert1.getEventReceivedTimestamp().getTime(), alert1.getAlertKey());

		StreamRecord<Alert> streamRecord2 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert2 = streamRecord2.getValue();
		Assert.assertEquals("Centos1", alert2.getMachineId());
		Assert.assertEquals("cpu", alert2.getResourceType());
		Assert.assertEquals("MC", alert2.getGroup());
		//Assert.assertEquals("1", alert2.getRuleId());
		//Assert.assertEquals("cpu_time_idle >= 90", alert2.getRuleExpr());
		//Assert.assertEquals("cpu_time_idle is exceeded", alert2.getMessage());
		Assert.assertEquals("{cpu_time_idle=70.04}", alert2.getActualValue());
		//Assert.assertEquals(Long.valueOf(0), alert2.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert2.getConsecutiveLimit());
		Assert.assertEquals("REVERSE", alert2.getAlertType());
		Assert.assertEquals("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert|"
				+ alert2.getEventReceivedTimestamp().getTime(), alert2.getAlertKey());
	}	

	@Test
	public void testDefaultRuleEvaluatorForSyslog() {
		DefaultRuleEvaluator defaultRuleEvaluator = new DefaultRuleEvaluator();
		JSONObject matchSyslogEventInput = TestDataGenerator.fetchRuleMatchSyslogEvent();
		JSONObject mismatchSyslogEventInput = TestDataGenerator.fetchRuleMismatchSyslogEvent();
		Rule syslogRuleInput = TestDataGenerator.fetchSyslogRule();
		String syslogPartitionKey = "{group_name=Syslog;resource_type=syslog;machine_name=zif4vm2}";
		String syslogRuleId = "2";

		Queue<Object> expectedOutput = applyTestHarness(defaultRuleEvaluator, syslogRuleInput, matchSyslogEventInput,
				mismatchSyslogEventInput, syslogPartitionKey, syslogRuleId);
		Assert.assertEquals(2, expectedOutput.size());

		StreamRecord<Alert> streamRecord1 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert1 = streamRecord1.getValue();
		Assert.assertEquals("zif4vm2", alert1.getMachineId());
		Assert.assertEquals("syslog", alert1.getResourceType());
		Assert.assertEquals("Syslog", alert1.getGroup());
		//Assert.assertEquals("2", alert1.getRuleId());
		//Assert.assertEquals("new RegExp('No living connections','g').test( body )", alert1.getRuleExpr());
		//Assert.assertEquals("test syslog", alert1.getMessage());
		Assert.assertEquals("{body=kibana: {\\\"type\\\":\\\"log\\\",\\\"@timestamp\\\":\\\"2020-11-09T07:45:21Z\\\",\\\"tags\\\":[\\\"warning\\\",\\\"elasticsearch\\\",\\\"data\\\"],\\\"pid\\\":591,\\\"message\\\":\\\"No living connections\\\"}}", alert1.getActualValue());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveLimit());
		Assert.assertEquals("POSITIVE", alert1.getAlertType());
		Assert.assertEquals("ruleId:2|machineId:zif4vm2|eventType:syslog|type:cep_alert|"
				+ alert1.getEventReceivedTimestamp().getTime(), alert1.getAlertKey());

		StreamRecord<Alert> streamRecord2 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert2 = streamRecord2.getValue();
		Assert.assertEquals("zif4vm2", alert2.getMachineId());
		Assert.assertEquals("syslog", alert2.getResourceType());
		Assert.assertEquals("Syslog", alert2.getGroup());
		//Assert.assertEquals("2", alert2.getRuleId());
		//Assert.assertEquals("new RegExp('No living connections','g').test( body )", alert2.getRuleExpr());
		//Assert.assertEquals("test syslog", alert2.getMessage());
		Assert.assertEquals("{body=kibana: {\\\"type\\\":\\\"log\\\",\\\"@timestamp\\\":\\\"2020-11-09T07:45:21Z\\\",\\\"tags\\\":[\\\"warning\\\",\\\"elasticsearch\\\",\\\"data\\\"],\\\"pid\\\":591,\\\"message\\\":\\\"Alive connections\\\"}}", alert2.getActualValue());
		//Assert.assertEquals(Long.valueOf(0), alert2.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert2.getConsecutiveLimit());
		Assert.assertEquals("REVERSE", alert2.getAlertType());
		Assert.assertEquals("ruleId:2|machineId:zif4vm2|eventType:syslog|type:cep_alert|"
				+ alert2.getEventReceivedTimestamp().getTime(), alert2.getAlertKey());
	}
	
	
	@Test
	public void testDefaultRuleEvaluatorForSnmp() {
		DefaultRuleEvaluator defaultRuleEvaluator = new DefaultRuleEvaluator();
		JSONObject matchSnmpEventInput = TestDataGenerator.fetchRuleMatchSnmpEvent();
		JSONObject mismatchSnmpEventInput = TestDataGenerator.fetchRuleMismatchSnmpEvent();
		Rule snmpRuleInput = TestDataGenerator.fetchSnmpRule();
		String snmpPartitionKey = "{group_name=SNMP;resource_type=snmp;machine_name=FW_ZA1_LOCKBOX}";
		String snmpRuleId = "3";

		Queue<Object> expectedOutput = applyTestHarness(defaultRuleEvaluator, snmpRuleInput, matchSnmpEventInput,
				mismatchSnmpEventInput, snmpPartitionKey, snmpRuleId);
		Assert.assertEquals(2, expectedOutput.size());

		StreamRecord<Alert> streamRecord1 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert1 = streamRecord1.getValue();
		Assert.assertEquals("FW_ZA1_LOCKBOX", alert1.getMachineId());
		Assert.assertEquals("snmptrap", alert1.getResourceType());
		Assert.assertEquals("SNMP", alert1.getGroup());
		//Assert.assertEquals("3", alert1.getRuleId());
		//Assert.assertEquals("new RegExp('alarmhasbeengenerated','g').test( description ) && new RegExp('NOGROUP','g').test( others )", alert1.getRuleExpr());
		//Assert.assertEquals("test snmp", alert1.getMessage());
		Assert.assertEquals("{description=ACAWAAE(instance:PRDmachine:PRD)alarmhasbeengenerated., others=503|1|PRD|NOBOX|PRD|NOGROUP|67327899|1|PRD}", alert1.getActualValue());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert1.getConsecutiveLimit());
		Assert.assertEquals("POSITIVE", alert1.getAlertType());
		Assert.assertEquals("ruleId:3|machineId:FW_ZA1_LOCKBOX|eventType:snmptrap|type:cep_alert|"
				+ alert1.getEventReceivedTimestamp().getTime(), alert1.getAlertKey());

		StreamRecord<Alert> streamRecord2 = (StreamRecord<Alert>) expectedOutput.poll();
		Alert alert2 = streamRecord2.getValue();
		Assert.assertEquals("FW_ZA1_LOCKBOX", alert2.getMachineId());
		Assert.assertEquals("snmptrap", alert2.getResourceType());
		Assert.assertEquals("SNMP", alert2.getGroup());
		//Assert.assertEquals("3", alert2.getRuleId());
		//Assert.assertEquals("new RegExp('alarmhasbeengenerated','g').test( description ) && new RegExp('NOGROUP','g').test( others )", alert2.getRuleExpr());
		//Assert.assertEquals("test snmp", alert2.getMessage());
		Assert.assertEquals("{description=ACAWAAE(instance:PRDmachine:PRD)alarmhasbeengenerated., others=503|1|PRD|NOBOX|PRD|YESGROUP|67327899|1|PRD}", alert2.getActualValue());
		//Assert.assertEquals(Long.valueOf(0), alert2.getConsecutiveCount());
		//Assert.assertEquals(Long.valueOf(3), alert2.getConsecutiveLimit());
		Assert.assertEquals("REVERSE", alert2.getAlertType());
		Assert.assertEquals("ruleId:3|machineId:FW_ZA1_LOCKBOX|eventType:snmptrap|type:cep_alert|"
				+ alert2.getEventReceivedTimestamp().getTime(), alert2.getAlertKey());
	}

	public Queue<Object> applyTestHarness(
			KeyedBroadcastProcessFunction<String, Keyed<JSONObject, String, String>, Rule, Alert> processFunction,
			Rule ruleInput, JSONObject matchEventInput, JSONObject mismatchEventInput, String partitionKey,
			String ruleId) {
		Queue<Object> expectedOutput = null;
		MapStateDescriptor<?, ?> descriptors = Descriptors.rulesDescriptor;
		KeySelector<Keyed<JSONObject, String, String>, String> keySelector = new KeySelector<Keyed<JSONObject, String, String>, String>() {
			@Override
			public String getKey(Keyed<JSONObject, String, String> value) {
				return value.getKey();
			}
		};
		try (KeyedBroadcastOperatorTestHarness<String, Keyed<JSONObject, String, String>, Rule, Alert> harness = ProcessFunctionTestHarnesses
				.forKeyedBroadcastProcessFunction(processFunction, keySelector, TypeInformation.of(String.class),
						descriptors)) {

			harness.open();
			MapState<String, Long> prevConsecutiveCountMapState = processFunction.getRuntimeContext()
					.getMapState(new MapStateDescriptor<>("prevConsecutiveCount", String.class, Long.class));
			MapState<String, Boolean> positiveAlertMapState = processFunction.getRuntimeContext()
					.getMapState(new MapStateDescriptor<>("alertStatus", String.class, Boolean.class));

			Keyed<JSONObject, String, String> keyedMatchEventInput = new Keyed<JSONObject, String, String>(
					matchEventInput, partitionKey, ruleId);
			Keyed<JSONObject, String, String> keyedMismatchEventInput = new Keyed<JSONObject, String, String>(
					mismatchEventInput, partitionKey, ruleId);

			harness.processBroadcastElement(ruleInput, 1);

			// Assert.assertEquals(false,prevConsecutiveCountMapState.contains("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert"));
			// Assert.assertEquals(false,
			// positiveAlertMapState.contains("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert"));
			// Assert.assertEquals(Long.valueOf(1),prevConsecutiveCountMapState.get("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert"));
			// Assert.assertEquals(null,
			// positiveAlertMapState.get("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert"));

			harness.processElement(keyedMatchEventInput, 2);
			harness.processElement(keyedMatchEventInput, 3);
			harness.processElement(keyedMatchEventInput, 4);
			harness.processElement(keyedMismatchEventInput, 5);

			expectedOutput = harness.getOutput();
		} catch (Exception e) {
			LOGGER.error("Unexpected error in RuleEvaluatorTest - testHarnessRuleEvaluator", e);
		}
		return expectedOutput;
	}

}
