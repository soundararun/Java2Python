/**
 * 
 */
package com.zif.cep.test;

import java.util.Arrays;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.zif.cep.core.DynamicKeyFunction;
import com.zif.cep.core.Keyed;
import com.zif.cep.core.Rule;
import com.zif.cep.core.StreamingJob.Descriptors;

/**
 * @author Vijay
 *
 */
public class DynamicKeyFunctionTest {

	@Test
	public void testCpuDynamicKey() {
		JSONObject cpuEventInput = TestDataGenerator.fetchRuleMatchCpuEvent();
		Rule cpuRuleInput = TestDataGenerator.fetchCpuRule();
		testDynamicKeyFunction(cpuEventInput, cpuRuleInput, "1", "{group_name=MC;resource_type=cpu;machine_name=Centos1}");
	}
	
	@Test
	public void testSyslogDynamicKey() {
		JSONObject syslogEventInput = TestDataGenerator.fetchRuleMatchSyslogEvent();
		Rule syslogRuleInput = TestDataGenerator.fetchSyslogRule();
		testDynamicKeyFunction(syslogEventInput, syslogRuleInput, "2", "{group_name=Syslog;resource_type=syslog;machine_name=zif4vm2}");
	}
	
	@Test
	public void testSnmpDynamicKey() {
		JSONObject snmpEventInput = TestDataGenerator.fetchRuleMatchSnmpEvent();
		Rule snmpRuleInput = TestDataGenerator.fetchSnmpRule();
		testDynamicKeyFunction(snmpEventInput, snmpRuleInput, "3", "{group_name=SNMP;resource_type=snmptrap;machine_name=FW_ZA1_LOCKBOX}");
	}

	public void testDynamicKeyFunction(JSONObject inputEvent, Rule inputRule, String expectedRuleId,
			String expectedKey) {
		DynamicKeyFunction dynamicKeyFunction = new DynamicKeyFunction("group_name,resource_type");
		int maxParallelism = 1;
		int numTasks = 1;
		int taskIdx = 0;
		MapStateDescriptor<?, ?> descriptors = Descriptors.rulesDescriptor;
		try (TwoInputStreamOperatorTestHarness<JSONObject, Rule, Keyed<JSONObject, String, String>> testHarness = new TwoInputStreamOperatorTestHarness<>(
				new CoBroadcastWithNonKeyedOperator<>(Preconditions.checkNotNull(dynamicKeyFunction),
						Arrays.asList(descriptors)),
				maxParallelism, numTasks, taskIdx);) {
			
			testHarness.open();
			testHarness.processElement2(inputRule, 1L);
			testHarness.processElement1(inputEvent, 2L);

			StreamRecord<? extends Keyed<JSONObject, String, String>> streamRecord = (StreamRecord<? extends Keyed<JSONObject, String, String>>) testHarness
					.getOutput().poll();
			Keyed<JSONObject, String, String> keyedOutput = streamRecord.getValue();
			JSONObject wrappedEvent = keyedOutput.getWrapped();
			String actualKey = keyedOutput.getKey();
			String actualRuleId = keyedOutput.getId();

			Assert.assertEquals(inputEvent, wrappedEvent);
			Assert.assertEquals(expectedKey, actualKey);
			Assert.assertEquals(expectedRuleId, actualRuleId);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
