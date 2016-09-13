package com.enow.persistence.mongodb.mapper;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;
import org.json.simple.JSONObject;

public class SimpleMongoMapper implements MongoMapper {
	private String[] fields;

	@Override
	public Document toDocument(ITuple input) {
		Document document = new Document();
		long time = System.currentTimeMillis();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		// JSONObject json = null;
		// String webhook = null;
		// Connect con = new
		// Connect("https://hooks.slack.com/services/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");

		for (String field : fields) {
			if (field.equals("jsonObject")) {
				JSONObject _jsonObject = (JSONObject) input.getValueByField("jsonObject");

				if (_jsonObject == null) {
					document.append(field, "error").append("time", dayTime.format(new Date(time)));
				} else {
					document.append(field, _jsonObject.toJSONString()).append("time", dayTime.format(new Date(time)));
				}

			} else if (field.equals("jsonArray")) {
				ArrayList<JSONObject> _jsonArray = (ArrayList<JSONObject>) input.getValueByField("jsonArray");

				if (_jsonArray == null) {
					document.append(field, "error").append("time", dayTime.format(new Date(time)));
				} else {
						document.append(field, _jsonArray.toString()).append("time",
								dayTime.format(new Date(time)));
				}
			} else {
				document.append(field, input.getValueByField(field)).append("time",
						dayTime.format(new Date(time)));

				// json = new JSONObject();
				// json.put("text", input.getValueByField(field).toString());
				// webhook = con.post(con.getURL(), json);
			}
		}
		return document;
	}

	public SimpleMongoMapper withFields(String... fields) {
		this.fields = fields;
		return this;
	}
}