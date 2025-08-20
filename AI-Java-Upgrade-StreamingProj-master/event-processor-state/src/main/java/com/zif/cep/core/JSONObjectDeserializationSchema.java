package com.zif.cep.core;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JSONObjectDeserializationSchema implements
      DeserializationSchema<JSONObject> {
	private static final Logger LOGGER = Logger.getLogger(JSONObjectDeserializationSchema.class);
    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public JSONObject deserialize(byte[] bytes){
    	JSONObject jsonObject = null;
        try {
        	jsonObject = objectMapper.readValue(bytes, JSONObject.class);
		} catch (IOException e) {
			LOGGER.error("Unexpected error in JSONObjectDeserializationSchema - deserialize", e);
		}
        return jsonObject;
    }

    public boolean isEndOfStream(JSONObject jsonObj) {
        return false;
    }

    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
