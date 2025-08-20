package com.zif.cep.core;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zif.cep.core.Alert.AlertType;

public class Rule {
	
	private static final Logger LOGGER = Logger.getLogger(Rule.class);
	
	@JsonProperty("id")
	public String ruleId;// i.e 3
	@JsonProperty("name")
	public String ruleName;// i.e. CPU
	@JsonProperty("description")
	public String ruleDescription;// i.e "High CPU utilization"
	@JsonProperty("message")
	public String ruleMessage;// i.e. "High CPU utilization"
	@JsonProperty("tool_name")
	public String toolName;// i.e. CPU
	@JsonProperty("group_name")
	public List<String> groupNames;// i.e. ["Gaval","Zif"]
	@JsonProperty("device_name")
	public List<String> deviceNames;// i.e. ["MC001","MC002"]	
	@JsonProperty("expression")
	public String ruleExpr;// i.e. "cpu_time_idle > 10"
	@JsonProperty("resource_type")
	public String resourceType;
	@JsonProperty("priority")
	public String priority;// i.e. "MAJOR"
	@JsonProperty("consecutive_limit")
	public Long consecutiveLimit;// i.e. 3
	@JsonProperty("status")
	public String status;// i.e."ACTIVE" / "INACTIVE"

	Rule(){		
	}
	
	private Rule(String ruleId, String ruleName, String ruleDescription, String ruleMessage, String toolName, List<String> groupNames, List<String> deviceNames, 
			String ruleExpr, String resourceType, String priority, Long consecutiveLimit, String status) {		
		this.ruleId = ruleId;
		this.ruleName = ruleName;
		this.ruleDescription = ruleDescription;
		this.ruleMessage = ruleMessage;
		this.toolName = toolName;
		this.groupNames = groupNames;
		this.deviceNames = deviceNames;		
		this.ruleExpr = ruleExpr;
		this.resourceType = resourceType;
		this.priority = priority;		
		this.consecutiveLimit = consecutiveLimit;
		this.status = status;
	}

	public String getRuleId() {
		return ruleId;
	}
	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}
	public String getRuleName() {
		return ruleName;
	}
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	public String getRuleDescription() {
		return ruleDescription;
	}
	public void setRuleDescription(String ruleDescription) {
		this.ruleDescription = ruleDescription;
	}
	public String getRuleMessage() {
		return ruleMessage;
	}
	public void setRuleMessage(String ruleMessage) {
		this.ruleMessage = ruleMessage;
	}
	public String getToolName() {
		return toolName;
	}
	public void setToolName(String toolName) {
		this.toolName = toolName;
	}
	public List<String> getGroupNames() {
		return groupNames;
	}
	public void setGroupNames(List<String> groupNames) {
		this.groupNames = groupNames;
	}
	public List<String> getDeviceNames() {
		return deviceNames;
	}
	public void setDeviceNames(List<String> deviceNames) {
		this.deviceNames = deviceNames;
	}
	public String getRuleExpr() {
		return ruleExpr;
	}
	public void setRuleExpr(String ruleExpr) {
		this.ruleExpr = ruleExpr;
	}
	public String getResourceType() {
		return resourceType;
	}
	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}
	public String getPriority() {
		return priority;
	}
	public void setPriority(String priority) {
		this.priority = priority;
	}
	public Long getConsecutiveLimit() {
		return consecutiveLimit;
	}
	public void setConsecutiveLimit(Long consecutiveLimit) {
		this.consecutiveLimit = consecutiveLimit;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}

	/*
	 * public String toString() { ObjectMapper objectMapper = new ObjectMapper();
	 * String ruleJsonStr = null; try { ruleJsonStr =
	 * objectMapper.writeValueAsString(this); } catch (JsonProcessingException e) {
	 * LOGGER.error("Unexpected error in Rule - toString", e); } return ruleJsonStr;
	 * }
	 */

	public static class Builder {
		String ruleId;// i.e 3
		String ruleName;// i.e. CPU
		String ruleDescription;// i.e "High CPU utilization"
		String ruleMessage;// i.e. "High CPU utilization"
		String toolName;// i.e. CPU
		List<String> groupNames;// i.e. ["Gaval","Zif"]
		List<String> deviceNames;// i.e. ["MC001","MC002"]	
		String ruleExpr;// i.e. "cpu_time_idle > 10"
		String resourceType;
		String priority;// i.e. "MAJOR"
		Long consecutiveLimit;// i.e. 3
		String status;// i.e."ACTIVE" / "INACTIVE"
		
		public Builder() {
		}
		public Rule build() {
			return new Rule(ruleId, ruleName, ruleDescription, ruleMessage, toolName, groupNames, deviceNames, ruleExpr, resourceType, priority, consecutiveLimit, status);
		}
		public Builder setRuleId(String ruleId) {
			this.ruleId = ruleId;
			return this;
		}
		public Builder setRuleName(String ruleName) {
			this.ruleName = ruleName;
			return this;
		}
		public Builder setRuleDescription(String ruleDescription) {
			this.ruleDescription = ruleDescription;
			return this;
		}
		public Builder setRuleMessage(String ruleMessage) {
			this.ruleMessage = ruleMessage;
			return this;
		}
		public Builder setToolName(String toolName) {
			this.toolName = toolName;
			return this;
		}		
		public Builder setGroupNames(List<String> groupNames) {
			this.groupNames = groupNames;
			return this;
		}		
		public Builder setDeviceNames(List<String> deviceNames) {
			this.deviceNames = deviceNames;
			return this;
		}
		public Builder setRuleExpr(String ruleExpr) {
			this.ruleExpr = ruleExpr;
			return this;
		}
		public Builder setResourceType(String resourceType) {
			this.resourceType = resourceType;
			return this;
		}
		public Builder setPriority(String priority) {
			this.priority = priority;
			return this;
		}
		public Builder setConsecutiveLimit(Long consecutiveLimit) {
			this.consecutiveLimit = consecutiveLimit;
			return this;
		}
		public Builder setStatus(String status) {
			this.status = status;
			return this;
		}		
	}
}
