package com.enow.storm.mapper.mongodb;

import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.storm.mongodb.common.mapper.MongoMapper;

import org.apache.storm.tuple.ITuple;
import org.bson.Document;

import com.enow.dto.TopicStructure;


public class mongoDBMapper implements MongoMapper {
	private String[] fields;
	private TopicStructure topicStructure = new TopicStructure();

	@Override
	public Document toDocument(ITuple input) {
		Document document = new Document();
		for (String field : fields) {
			if (field.equals("topicStructure")) {
				topicStructure = (TopicStructure) input.getValueByField("topicStructure");

				long time = System.currentTimeMillis();
				SimpleDateFormat dayTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				document.append("time", dayTime.format(new Date(time))).append("topic", topicStructure.output());
				//document.put("topic/msg", topicStructure.output());
			}else{
			}
		}
		return document;
	}

	public mongoDBMapper withFields(String... fields) {
		this.fields = fields;
		return this;
	}
}
