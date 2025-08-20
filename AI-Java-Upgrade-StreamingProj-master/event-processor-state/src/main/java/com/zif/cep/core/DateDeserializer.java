package com.zif.cep.core;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class DateDeserializer extends JsonDeserializer<Date> {
	private static final Logger LOGGER = Logger.getLogger(DateDeserializer.class);
	final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
	final TimeZone utc = TimeZone.getTimeZone("UTC");

	@Override
	public Date deserialize(JsonParser jsonparser, DeserializationContext deserializationcontext) throws IOException {
		String date = jsonparser.getText();
		try {
			dateFormat.setTimeZone(utc);
			return dateFormat.parse(date);
		} catch (ParseException e) {
			LOGGER.error("Unexpected error in DateDeserializer - formatting error", e);			
			throw new RuntimeException(e);
		}
	}
}