package com.enow.storm.TriggerTopology;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.enow.dto.TopicStructure;
import com.enow.storm.Connect;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;

public class StagingBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
	private OutputCollector collector;
	private TopicStructure topicStructure;
	String msg = null;
	boolean machineIdCheck = false;
	boolean phaseRoadMapIdCheck = false;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		topicStructure = new TopicStructure();
	}

	@Override
	public void execute(Tuple input) {
		topicStructure = (TopicStructure) input.getValueByField("topicStructure");
		msg = input.getStringByField("msg");
		if (null == topicStructure) {
			return;
		} else if ((null == msg || msg.length() == 0)) {
			return;
		}
		// check Device ID
		MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );

		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase dbWrite = mongoClient.getDatabase("enow");
		MongoCollection<Document> deviceListCollection = dbWrite.getCollection("device");

		if (deviceListCollection.count(new Document("deviceId", topicStructure.getDeviceId())) == 0) {
			machineIdCheck = false;
		} else if (deviceListCollection.count(new Document("deviceId", topicStructure.getDeviceId())) == 1) {
			machineIdCheck = true;
		} else {
			// machineIdCheck = "device id : now we have a problem";
			LOG.debug("There are more than two machine ID on MongoDB");
		}
		// check Phase Road-map ID
		MongoCollection<Document> phaseRoadMapCollection = dbWrite.getCollection("phaseRoadMap");

		try {
			if (phaseRoadMapCollection
					.count(new Document("phaseRoadMapId", Integer.parseInt(topicStructure.getPhaseRoadMapId()))) == 0) {
				phaseRoadMapIdCheck = false;
			} else if (phaseRoadMapCollection
					.count(new Document("phaseRoadMapId", Integer.parseInt(topicStructure.getPhaseRoadMapId()))) == 1) {
				phaseRoadMapIdCheck = true;
			} else {
				// phaseRoadMapIdCheck = "phase road map id : now we have a
				// problem";
				LOG.debug("There are more than two Phase Roadmap Id on MongoDB");
			}
		} catch (NumberFormatException e) {
			e.getMessage();
			phaseRoadMapIdCheck = false;
		}

		FindIterable<Document> iterable = phaseRoadMapCollection.find(new Document("phaseRoadMapId", 1));

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObject;
				try {
					// JSON데이터를 넣어 JSON Object 로 만들어 준다.
					jsonObject = (JSONObject) jsonParser.parse(document.toJson());
					jsonObject.get("").toString();
					
					
					
					JSONObject json = null;
					String webhook = null;
					Connect con = new Connect("https://hooks.slack.com/services/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");

					json = new JSONObject();
					json.put("text", jsonObject.get("phaseId").toString());
					webhook = con.post(con.getURL(), json);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		collector.emit(new Values(topicStructure, msg, machineIdCheck, phaseRoadMapIdCheck));

		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topicStructure", "msg", "machineIdCheck", "phaseRoadMapIdCheck"));
	}
}
