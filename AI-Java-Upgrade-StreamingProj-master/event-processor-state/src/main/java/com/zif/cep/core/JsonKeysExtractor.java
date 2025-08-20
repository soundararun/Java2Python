package com.zif.cep.core;

import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONObject;

/** Utilities for dynamic keys extraction by field name. */
public class JsonKeysExtractor {

	/**
	 * Extracts and concatenates field values by names.
	 *
	 * @param keyNames list of field names
	 * @param object   target for values extraction
	 */
	public static String getKey(List<String> keyNames, JSONObject jsonObj)
			throws NoSuchFieldException, IllegalAccessException {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		if (keyNames.size() > 0) {
			Iterator<String> it = keyNames.iterator();
			appendKeyValue(sb, jsonObj, it.next());

			while (it.hasNext()) {
				sb.append(";");
				appendKeyValue(sb, jsonObj, it.next());
			}
			sb.append(";");
			appendKeyValue(sb, jsonObj, "machine_name");
		}
		sb.append("}");
		return sb.toString();
	}

	private static void appendKeyValue(StringBuilder sb, JSONObject jsonObj, String fieldName)
			throws IllegalAccessException, NoSuchFieldException {
		sb.append(fieldName);
		sb.append("=");
		sb.append((String) jsonObj.get(fieldName));
	}
}
