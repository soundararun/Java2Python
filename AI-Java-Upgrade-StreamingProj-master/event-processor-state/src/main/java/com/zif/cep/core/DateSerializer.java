package com.zif.cep.core;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class DateSerializer extends JsonSerializer<Date> {	
	private static final Logger LOGGER = Logger.getLogger(DateDeserializer.class);
	final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
	final TimeZone utc = TimeZone.getTimeZone("UTC");
	
	@Override
	public void serialize(Date date, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		 try {			 
			  dateFormat.setTimeZone(utc);
	          String formattedDate  = dateFormat.format(date);
	          gen.writeString(formattedDate);
	      } catch (JsonProcessingException  e) {
	    	  LOGGER.error("Unexpected error in DateSerializer - serialize", e);
	          gen.writeString("");
	      }
		
	}   
}