package com.zif.cep.core;

import java.io.IOException;

import static com.zif.cep.core.ProcessingUtils.appendRuleExprIfAny;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class RuleDeserializationSchema implements DeserializationSchema<Rule> {
	private static final Logger LOGGER = Logger.getLogger(RuleDeserializationSchema.class);

	static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

	public Rule deserialize(byte[] bytes) {
		Rule rule = null;
		try {
			rule = objectMapper.readValue(bytes, Rule.class);
			appendRuleExprIfAny(rule);
		} catch (IOException e) {
			LOGGER.error("Unexpected error in RuleDeserializationSchema - deserialize", e);
		}
		return rule;
	}

	public boolean isEndOfStream(Rule rule) {
		return false;
	}

	public TypeInformation<Rule> getProducedType() {
		return TypeInformation.of(Rule.class);
	}
}
