package com.zif.cep.core;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AlertSerializationSchema implements SerializationSchema<Alert> {
	private static final Logger LOGGER = Logger.getLogger(AlertSerializationSchema.class);

	static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

	SimpleFilterProvider filterProvider = new SimpleFilterProvider();
	Set<String> alertFilterList = Arrays
			.asList("alertId", "deviceName", "alertType", "appName", "alertDescription", "actual", "receivedTimeUTC",
					"generatedTimeUTC", "alertSeverity", "toolName", "others", "counterName", "gtmCaseId", "gtmStatus")
			.stream().collect(Collectors.toSet());

	@Override
	public byte[] serialize(Alert alert) {
		if (objectMapper == null) {
			objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
			objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
			// filterProvider.addFilter("alertFilter",
			// SimpleBeanPropertyFilter.filterOutAllExcept(alertFilterList));
			// objectMapper.setFilterProvider(filterProvider);
		}
		try {
			String json = objectMapper.writeValueAsString(alert);
			return json.getBytes();
		} catch (com.fasterxml.jackson.core.JsonProcessingException e) {
			LOGGER.error("Unexpected error in AlertSerializationSchema - Failed to parse JSON", e);
		}
		return new byte[0];
	}
}
