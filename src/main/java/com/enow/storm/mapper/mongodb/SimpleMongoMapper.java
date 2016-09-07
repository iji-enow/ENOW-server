package com.enow.storm.mapper.mongodb;

import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;

import com.enow.dto.TopicStructure;

public class SimpleMongoMapper implements MongoMapper {
	private String[] fields;

	@Override
	public Document toDocument(ITuple input) {
		Document document = new Document();
		TopicStructure topicStructure = new TopicStructure();
		//JSONObject json = null;
		//String webhook = null;
		//Connect con = new Connect("https://hooks.slack.com/services/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");
		
		for (String field : fields) {
			if (field.equals("topicStructure")) {
				topicStructure = (TopicStructure) input.getValueByField("topicStructure");

				long time = System.currentTimeMillis();
				SimpleDateFormat dayTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				document.append("time", dayTime.format(new Date(time))).append("topic", topicStructure.output());

				//json = new JSONObject();
				//json.put("text", field);
				//webhook = con.post(con.getURL(), json);

			} else {
				document.append(field, input.getValueByField(field));

				//json = new JSONObject();
				//json.put("text", input.getValueByField(field).toString());
				//webhook = con.post(con.getURL(), json);
			}
		}
		return document;
	}

	public SimpleMongoMapper withFields(String... fields) {
		this.fields = fields;
		return this;
	}
}