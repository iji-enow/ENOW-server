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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.enow.storm.Connect;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IndexingBolt extends BaseRichBolt {
	protected static final Logger LOG = LogManager.getLogger(IndexingBolt.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		JSONParser parser = new JSONParser();
		JSONObject _jsonObject;
		boolean deviceIdCheck = false;
		boolean roadMapIdCheck = false;
		boolean mapIdCheck = false;
		boolean brokerIdCheck = false;
		boolean serverIdCheck = false;

		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);

		JSONObject error = new JSONObject();

		if (input.toString().length() == 0) {
			error.put("error", "indexingBolt/1");
			//collector.emit(new Values(error));
			return;
		}

		String msg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

		if (input.getSourceComponent().equals("event-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("corporationName") && _jsonObject.containsKey("serverId")
						&& _jsonObject.containsKey("brokerId") && _jsonObject.containsKey("roadMapId")) {
					_jsonObject.put("spoutName", "event");
				} else {
					error.put("error", "indexingBolt/2");
					//collector.emit(new Values(error));
					return;
				}
			} catch (ParseException e) {
				error.put("error", "indexingBolt/6");
				//collector.emit(new Values(error));
				return;
			}
		} else if (input.getSourceComponent().equals("order-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("corporationName") && _jsonObject.containsKey("serverId")
						&& _jsonObject.containsKey("brokerId") && _jsonObject.containsKey("roadMapId")
						&& _jsonObject.containsKey("deviceId") && _jsonObject.containsKey("payload")) {
				} else {
					error.put("error", "indexingBolt/3");
					//collector.emit(new Values(error));
					return;
				}

				MongoDatabase dbWrite = mongoClient.getDatabase("lists");

				MongoCollection<Document> serverListCollection = dbWrite.getCollection("server");

				if (serverListCollection.count(new Document("serverId", (String) _jsonObject.get("serverId"))) == 0) {
					serverIdCheck = false;
				} else if (serverListCollection
						.count(new Document("serverId", (String) _jsonObject.get("serverId"))) == 1) {
					serverIdCheck = true;
				} else {
					serverIdCheck = false;
					LOG.debug("There are more than two server ID on MongoDB");
				}

				// Get device collection for matching input signal from device
				MongoCollection<Document> brokerListCollection = dbWrite.getCollection("broker");

				if (brokerListCollection.count(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 0) {
					brokerIdCheck = false;
				} else if (brokerListCollection
						.count(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 1) {
					brokerIdCheck = true;

				} else {
					brokerIdCheck = false;
					LOG.debug("There are more than two broker ID on MongoDB");
				}

				// Get device collection for matching input signal from device

				MongoCollection<Document> deviceListCollection = dbWrite.getCollection("device");

				if (deviceListCollection.count(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 0) {
					deviceIdCheck = false;
				} else if (deviceListCollection
						.count(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 1) {
					deviceIdCheck = true;
				} else {
					deviceIdCheck = false;
					LOG.debug("There are more than two machine ID on MongoDB");
				}

				dbWrite = mongoClient.getDatabase("enow");

				MongoCollection<Document> roadMapCollection = dbWrite.getCollection("roadMap");

				if (roadMapCollection.count(new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 0) {
					roadMapIdCheck = false;
				} else if (roadMapCollection
						.count(new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 1) {
					roadMapIdCheck = true;
				} else {
					roadMapIdCheck = false;
					LOG.debug("There are more than two Phase Road-map Id on MongoDB");
				}

				if (serverIdCheck && brokerIdCheck && deviceIdCheck && roadMapIdCheck) {
					_jsonObject.put("spoutName", "order");
				} else {
					error.put("error", "indexingBolt/4");
					//collector.emit(new Values(error));
					return;
				}
			} catch (ParseException e) {
				error.put("error", "indexingBolt/6");
				//collector.emit(new Values(error));
				return;
			}

		} else if (input.getSourceComponent().equals("proceed-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("order") && _jsonObject.containsKey("roadMapId")
						&& _jsonObject.containsKey("mapId") && _jsonObject.containsKey("payload")
						&& _jsonObject.containsKey("incomingNode") && _jsonObject.containsKey("outingNode")
						&& _jsonObject.containsKey("previousData") && _jsonObject.containsKey("topic")
						&& _jsonObject.containsKey("lastNode")) {

					_jsonObject.put("spoutName", "proceed");
				} else {
					// init = false 일 경우 필요한 값이 다 안 들어 왔다.
					error.put("error", "indexingBolt/5");
					//collector.emit(new Values(error));
					return;
				}
			} catch (ParseException e) {
				error.put("error", "indexingBolt/6");
				//collector.emit(new Values(error));
				return;
			}
		} else {
			error.put("error", "indexingBolt/7");
			//collector.emit(new Values(error));
			return;
		}

		/*
		 * try { _jsonObject = (JSONObject) parser.parse(msg); if
		 * (_jsonObject.containsKey("init")) { if ((boolean)
		 * _jsonObject.get("init")) { if
		 * (_jsonObject.containsKey("corporationName") &&
		 * _jsonObject.containsKey("serverId") &&
		 * _jsonObject.containsKey("brokerId") &&
		 * _jsonObject.containsKey("roadMapId")) { collector.emit(new
		 * Values(_jsonObject)); } else { // init = true 일 경우 필요한 값이 다 안 들어 왔다.
		 * error.put("error", "indexingBolt/2"); collector.emit(new
		 * Values(error)); } } else { if (_jsonObject.containsKey("proceed") &&
		 * _jsonObject.containsKey("corporationName") &&
		 * _jsonObject.containsKey("roadMapId") &&
		 * _jsonObject.containsKey("mapId") &&
		 * _jsonObject.containsKey("payload") &&
		 * _jsonObject.containsKey("incomingNode") &&
		 * _jsonObject.containsKey("outingNode") &&
		 * _jsonObject.containsKey("previousData") &&
		 * _jsonObject.containsKey("topic") &&
		 * _jsonObject.containsKey("lastNode")) { collector.emit(new
		 * Values(_jsonObject)); } else { // init = false 일 경우 필요한 값이 다 안 들어 왔다.
		 * error.put("error", "indexingBolt/3"); collector.emit(new
		 * Values(error)); return; } } } else { // init자체가 안들어왔다.
		 * error.put("error", "indexingBolt/4"); collector.emit(new
		 * Values(error)); } } catch (ParseException e) { // JSONParseException
		 * 발 error.put("error", "indexingBolt/5"); collector.emit(new
		 * Values(error)); }
		 */
		/*
		 * MongoDatabase dbWrite = mongoClient.getDatabase("lists");
		 * 
		 * MongoCollection<Document> serverListCollection =
		 * dbWrite.getCollection("server");
		 * 
		 * if (serverListCollection.count(new Document("serverId", (String)
		 * _jsonObject.get("serverId"))) == 0) { // There isn't deviceId that
		 * matches input signal from // device serverIdCheck = false; } else if
		 * (serverListCollection.count(new Document("serverId", (String)
		 * _jsonObject.get("serverId"))) == 1) { // There is deviceId that
		 * matches input signal from device serverIdCheck = true; } else {
		 * serverIdCheck = false;
		 * LOG.debug("There are more than two server ID on MongoDB"); }
		 * 
		 * // Get device collection for matching input signal from device
		 * MongoCollection<Document> brokerListCollection =
		 * dbWrite.getCollection("broker");
		 * 
		 * if (brokerListCollection.count(new Document("brokerId", (String)
		 * _jsonObject.get("brokerId"))) == 0) { // There isn't deviceId that
		 * matches input signal from // device brokerIdCheck = false; } else if
		 * (brokerListCollection.count(new Document("brokerId", (String)
		 * _jsonObject.get("brokerId"))) == 1) { // There is deviceId that
		 * matches input signal from device brokerIdCheck = true;
		 * 
		 * } else { brokerIdCheck = false;
		 * LOG.debug("There are more than two broker ID on MongoDB"); }
		 * 
		 * // Get device collection for matching input signal from device
		 * 
		 * MongoCollection<Document> deviceListCollection =
		 * dbWrite.getCollection("device");
		 * 
		 * if (deviceListCollection.count(new Document("deviceId", (String)
		 * _jsonObject.get("deviceId"))) == 0) { // There isn't deviceId that
		 * matches input signal from // device deviceIdCheck = false; } else if
		 * (deviceListCollection.count(new Document("deviceId", (String)
		 * _jsonObject.get("deviceId"))) == 1) { // There is deviceId that
		 * matches input signal from device deviceIdCheck = true; } else {
		 * deviceIdCheck = false;
		 * LOG.debug("There are more than two machine ID on MongoDB"); } //
		 * Check Phase Road-map ID
		 * 
		 * dbWrite = mongoClient.getDatabase("enow");
		 * 
		 * MongoCollection<Document> roadMapCollection =
		 * dbWrite.getCollection("roadMap");
		 * 
		 * if (roadMapCollection.count(new Document("roadMapId", (String)
		 * _jsonObject.get("roadMapId"))) == 0) { // There isn't phaseRoadMapId
		 * that matches input signal from // device roadMapIdCheck = false; }
		 * else if (roadMapCollection.count(new Document("roadMapId", (String)
		 * _jsonObject.get("roadMapId"))) == 1) { // There is phaseRoadMapId
		 * that matches input signal from // device roadMapIdCheck = true;
		 * 
		 * } else { roadMapIdCheck = false;
		 * LOG.debug("There are more than two Phase Road-map Id on MongoDB"); }
		 */

		/*
		 * if (serverIdCheck && brokerIdCheck && deviceIdCheck &&
		 * roadMapIdCheck) { collector.emit(new Values(_jsonObject)); } else {
		 * json = new JSONObject(); json.put("text", "6"); webhook =
		 * con.post(con.getURL(), json); return; }
		 */
		collector.emit(new Values(_jsonObject));

		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonObject"));
	}
}