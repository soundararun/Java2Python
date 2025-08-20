package com.zif.cep.test;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.zif.cep.config.Config;
import com.zif.cep.core.Alert;
import com.zif.cep.core.StreamingJob;
import com.zif.cep.rule.DefaultRuleEvaluator;
import com.zif.cep.test.LinesFromFileSource.EventFromFileSource;
import com.zif.cep.test.LinesFromFileSource.RuleFromFileSource;

public class StreamingJobTest {

    private StreamExecutionEnvironment env;
    private EventFromFileSource eventSource;
    private RuleFromFileSource ruleSource;    
    private AlertCollectingSink alertSink;
    private DefaultRuleEvaluator ruleEvaluator;
    private Config config;
    
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ruleSource = new RuleFromFileSource("src/test/resources/rules.txt",1L);
        eventSource = new EventFromFileSource("src/test/resources/events.txt",1000L);        
        alertSink = new AlertCollectingSink();
        ruleEvaluator = new DefaultRuleEvaluator();
        config = StreamingJob.loadConfig(new String[] {"--partition-key group_name,resource_type"});
    }

    
    @Test
    public void testCEP() throws Exception {
        new StreamingJob(eventSource, ruleSource, alertSink,ruleEvaluator,config).build(env);
        env.execute();
        Map<Date, Alert> alertMap = alertSink.getValues();
        assertEquals(6, alertMap.size()); 	    
        
        Map<Date, Alert> alertMapSortByDate = alertMap.entrySet()
        		  .stream()
        		  .sorted(Map.Entry.comparingByKey())
        		  .collect(Collectors.toMap(
        		    Map.Entry::getKey, 
        		    Map.Entry::getValue, 
        		    (oldValue, newValue) -> oldValue, TreeMap::new));

        Alert[] alertArray = alertMapSortByDate.values().stream().toArray(Alert[]::new);
        assertEquals("ruleId:1|machineId:Centos1|eventType:cpu|type:cep_alert|1587494561000", alertArray[0].getAlertKey());
        assertEquals("POSITIVE", alertArray[0].getAlertType());
        assertEquals("ruleId:1|machineId:Centos2|eventType:cpu|type:cep_alert|1587494561000", alertArray[1].getAlertKey());
        assertEquals("POSITIVE", alertArray[1].getAlertType());
        assertEquals("ruleId:2|machineId:Centos3|eventType:cpu|type:cep_alert|1587494561000", alertArray[2].getAlertKey());
        assertEquals("POSITIVE", alertArray[2].getAlertType());
        assertEquals("ruleId:2|machineId:Centos3|eventType:cpu|type:cep_alert|1587494561000", alertArray[3].getAlertKey());
        assertEquals("REVERSE", alertArray[3].getAlertType());
        assertEquals("ruleId:3|machineId:zif4vm2|eventType:syslog|type:cep_alert|1604907921000", alertArray[4].getAlertKey());
        assertEquals("POSITIVE", alertArray[4].getAlertType());
        assertEquals("ruleId:4|machineId:FW_ZA1_LOCKBOX|eventType:snmptrap|type:cep_alert|1587494561000", alertArray[5].getAlertKey());
        assertEquals("POSITIVE", alertArray[5].getAlertType());
    }   


    @After
    public void tearDown() {
    	alertSink.clear();
    }
}
