package com.zif.cep.core;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Bean class for generated alerts
 * 
 * @author Vijay
 *
 */

//@JsonFilter("alertFilter")
@JsonIgnoreProperties({"ruleId", "consecutiveLimit", "consecutiveCount"})
public class Alert {

	@JsonProperty("actual")
	public String actualValue;// i.e. 91
	@JsonProperty("alertType")
	public String resourceType;// i.e. cpu
	@JsonProperty("alertSeverity")
	public String alertType;// WARNING, POSITIVE, REVERSE
	@JsonProperty("appName")
	public String group;// i.e. MC
	@JsonProperty("alertId")
	private String alertKey;
	@JsonDeserialize(using = DateDeserializer.class)
	@JsonSerialize(using = DateSerializer.class)
	@JsonProperty("receivedTimeUTC")
	public Date eventReceivedTimestamp;// i.e. 2020-06-24T06:31:32Z
	@JsonDeserialize(using = DateDeserializer.class)
	@JsonSerialize(using = DateSerializer.class)
	@JsonProperty("generatedTimeUTC")
	private Date alertGeneratedTimestamp;// i.e. 2020-06-24T06:31:32Z
	@JsonProperty("deviceName")
	public String machineId;// i.e. centos1
	@JsonProperty("alertDescription")
	private String description;// i.e. test
	@JsonProperty("others")
	public String others;
	@JsonProperty("toolName")
	public String toolName;// Solarwinds
	@JsonProperty("counterName")
	public String counterName;
	@JsonProperty("gtmCaseId")
	public String gtmCaseId;
	@JsonProperty("gtmStatus")
	public String gtmStatus;
	@JsonIgnore
	public String ruleId;
	@JsonIgnore
	public Long consecutiveLimit;
	@JsonIgnore
	public Long consecutiveCount;

	/**
	 * @JsonProperty("rule_id") public String ruleId;// i.e.
	 * R1 @JsonProperty("rule_expression") public String ruleExpr;// i.e.
	 * cpu_idle_time <= 90 @JsonProperty("message") public String message;// i.e.
	 * CPU threshold exceeded @JsonProperty("consecutive_limit") public Long
	 * consecutiveLimit;// i.e. 3 @JsonProperty("consecutive_count") public Long
	 * consecutiveCount;// i.e. 3 @JsonProperty("expiry_time_in_millis") public Long
	 * expiryTimeInMillis;// i.e 10000 @JsonProperty("alertLabelId") public String
	 * alertLabelId; @JsonProperty("alertLabelText") public String
	 * alertLabelText; @JsonProperty("alertStatus") public String
	 * alertStatus; @JsonProperty("caseId") public String
	 * caseId; @JsonProperty("mappedSeverity") public String
	 * mappedSeverity; @JsonProperty("deviceMaint") public String deviceMaint;
	 */

	public Alert() {

	}

	Alert(String alertKey, String machineId, String resourceType, String group, String actualValue,
			Date eventReceivedTimestamp, Date alertGeneratedTimestamp, String alertType, String toolName,
			String description, String others, String counterName, String gtmCaseId, String gtmStatus) {
		this.alertKey = alertKey;
		this.machineId = machineId;
		this.resourceType = resourceType;
		this.group = group;
		this.actualValue = actualValue;
		this.eventReceivedTimestamp = eventReceivedTimestamp;
		this.alertGeneratedTimestamp = alertGeneratedTimestamp;
		this.alertType = alertType;
		this.others = others;
		this.counterName = counterName;
		this.toolName = toolName;
		this.description = description;
		this.gtmCaseId = gtmCaseId;
		this.gtmStatus = gtmStatus;
	}

	public String getAlertKey() {
		return alertKey;
	}

	public String getGroup() {
		return group;
	}

	public String getDescription() {
		return description;
	}

	public String getActualValue() {
		return actualValue;
	}

	public Date getEventReceivedTimestamp() {
		return eventReceivedTimestamp;
	}

	public Date getAlertGeneratedTimestamp() {
		return alertGeneratedTimestamp;
	}

	public String getAlertType() {
		return alertType;
	}

	public void setAlertType(String alertType) {
		this.alertType = alertType;
	}

	public String getResourceType() {
		return resourceType;
	}
	
	public Long getConsecutiveLimit() {
		return consecutiveLimit;
	}
	
	public void setConsecutiveCount(Long consecutiveCount) {
		this.consecutiveCount = consecutiveCount;
	}

	public Long getConsecutiveCount() {
		return consecutiveCount;
	}

	public String getRuleId() {
		return ruleId;
	}

	public String getToolName() {
		return toolName;
	}
	
	public String getMachineId() {
		return machineId;
	}

	public String getOthers() {
		return others;
	}

	public String getCounterName() {
		return counterName;
	}

	public String getGtmCaseId() {
		return gtmCaseId;
	}

	public String getGtmStatus() {
		return gtmStatus;
	}

	public static class Builder {
		String alertKey;
		String machineId;// i.e. centos1
		String resourceType;// i.e. cpu
		String group;// i.e. MC
		String actualValue;// i.e. 91
		Date eventReceivedTimestamp;// i.e. 2020-06-24T06:31:32Z
		Date alertGeneratedTimestamp;// 3
		AlertType alertType;
		String toolName;
		String description;
		String others;
		String counterName;
		String gtmCaseId;
		String gtmStatus;

		public Builder() {
		}

		public Alert build() {
			return new Alert(alertKey, machineId, resourceType, group, actualValue, eventReceivedTimestamp,
					alertGeneratedTimestamp, alertType.toString(), toolName, description, others, counterName,
					gtmCaseId, gtmStatus);
		}

		public Builder setAlertKey(String alertKey) {
			this.alertKey = alertKey;
			return this;
		}

		public Builder setMachineId(String machineId) {
			this.machineId = machineId;
			return this;
		}

		public Builder setResourceType(String resourceType) {
			this.resourceType = resourceType;
			return this;
		}

		public Builder setGroup(String group) {
			this.group = group;
			return this;
		}

		public Builder setActualValue(String actualValue) {
			this.actualValue = actualValue;
			return this;
		}

		public Builder setEventReceivedTimestamp(Date eventReceivedTimestamp) {
			this.eventReceivedTimestamp = eventReceivedTimestamp;
			return this;
		}

		public Builder setAlertGeneratedTimestamp(Date alertGeneratedTimestamp) {
			this.alertGeneratedTimestamp = alertGeneratedTimestamp;
			return this;
		}

		public Builder setAlertType(String alertTypeStr) {
			this.alertType = AlertType.valueOf(alertTypeStr);
			return this;
		}

		public Builder setToolName(String toolName) {
			this.toolName = toolName;
			return this;
		}

		public Builder setDescription(String description) {
			this.description = description;
			return this;
		}

		public Builder setOthers(String others) {
			this.others = others;
			return this;
		}

		public Builder setCounterName(String counterName) {
			this.counterName = counterName;
			return this;
		}

		public Builder setGtmCaseId(String gtmCaseId) {
			this.gtmCaseId = gtmCaseId;
			return this;
		}

		public Builder setGtmStatus(String gtmStatus) {
			this.gtmStatus = gtmStatus;
			return this;
		}

		/*
		 * public String toString() { ObjectMapper objectMapper = new ObjectMapper();
		 * String alertJsonStr = null; try { alertJsonStr =
		 * objectMapper.writeValueAsString(this); } catch (JsonProcessingException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } return
		 * alertJsonStr; }
		 */

	}

	public enum AlertType {
		WARNING, POSITIVE, REVERSE
	}
}
