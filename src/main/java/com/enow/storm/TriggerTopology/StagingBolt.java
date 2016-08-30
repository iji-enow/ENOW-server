package com.enow.storm.TriggerTopology;

import java.util.Map;

import com.enow.dto.TopicStructure;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
		topicStructure = (TopicStructure) input.getValueByField("topicStucture");
		msg = input.getStringByField("msg");
		if (null == topicStructure) {
			return;
		} else if ((null == msg || msg.length() == 0)) {
			return;
		}
		// check Device ID
		MongoClient mongoClient = new MongoClient("52.193.56.228", 9092);

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
			if (phaseRoadMapCollection.count(new Document("phaseRoadMapId", topicStructure.getPhaseRoadMapId())) == 0) {
				phaseRoadMapIdCheck = false;
			} else if (phaseRoadMapCollection
					.count(new Document("phaseRoadMapId", topicStructure.getPhaseRoadMapId())) == 1) {
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
		declarer.declare(new Fields("topicStucture", "msg", "machineIdCheck", "phaseRoadMapIdCheck"));
	}
}
