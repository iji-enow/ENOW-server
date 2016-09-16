package com.enow.storm.TriggerTopology;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.enow.storm.Connect;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class StagingBolt extends BaseRichBolt {
	protected static final Logger LOG = LogManager.getLogger(StagingBolt2.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		JSONObject _jsonObject = null;
		boolean mapIdCheck = false;
		FindIterable<Document> iterable;
		JSONParser jsonParser = new JSONParser();
		JSONObject roadMapId;
		JSONObject mapIds;
		JSONObject mapId;
		JSONObject phaseId;
		JSONObject devices;
		JSONObject outingNode;
		JSONObject incomingNode;
		JSONObject subsequentInitPeer;
		JSONArray deviceId;
		JSONArray incomingNodeArray;
		JSONArray subsequentInitPeerPhase;
		JSONArray outingNodeArray;
		JSONArray incomingPeerArray;
		JSONArray initNodeArray;
		JSONArray lastNodeArray;
		JSONParser parser = new JSONParser();
		JSONObject error = new JSONObject();

		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();
		ConcurrentHashMap<String, JSONObject> ackSchdueling = new ConcurrentHashMap<>();
		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);

		_jsonObject = (JSONObject) input.getValueByField("jsonObject");

		if (_jsonObject.containsKey("error")) {
			_jsonArray.add(_jsonObject);
			LOG.debug("error : stagingBolt/1");
			// collector.emit(new Values(_jsonArray));
			return;
		}

		MongoDatabase dbWrite = mongoClient.getDatabase("enow");
		MongoCollection<Document> roadMapCollection = dbWrite.getCollection("roadMap");

		iterable = roadMapCollection.find(new Document("roadMapId", (String) _jsonObject.get("roadMapId")));

		if (_jsonObject.get("spoutName").equals("event")) {
			try {
				roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

				mapIds = (JSONObject) roadMapId.get("mapIds");
				initNodeArray = (JSONArray) roadMapId.get("initNode");
				incomingNode = (JSONObject) roadMapId.get("incomingNode");
				outingNode = (JSONObject) roadMapId.get("outingNode");
				lastNodeArray = (JSONArray) roadMapId.get("lastNode");

				String jsonString = _jsonObject.toJSONString();

				for (int i = 0; i < initNodeArray.size(); i++) {
					String InitMapId = (String) initNodeArray.get(i);

					mapId = (JSONObject) mapIds.get(InitMapId);

					JSONObject tmpJsonObject = new JSONObject();

					tmpJsonObject = (JSONObject) parser.parse(jsonString);
					tmpJsonObject.put("payload", null);
					tmpJsonObject.put("previousData", null);
					tmpJsonObject.put("order", false);
					tmpJsonObject.put("mapId", InitMapId);
					tmpJsonObject.put("topic", "enow" + "/" + mapId.get("serverId") + "/"
							+ mapId.get("brokerId") + "/" + mapId.get("deviceId"));

					tmpJsonObject.remove("spoutName");

					if (outingNode.containsKey(InitMapId)) {
						outingNodeArray = (JSONArray) outingNode.get(InitMapId);

						tmpJsonObject.put("outingNode", outingNodeArray);
						tmpJsonObject.put("lastNode", false);
					} else {
						tmpJsonObject.put("outingNode", null);
						tmpJsonObject.put("lastNode", true);
					}

					if (incomingNode.containsKey(InitMapId)) {
						incomingNodeArray = (JSONArray) incomingNode.get(InitMapId);

						tmpJsonObject.put("incomingNode", incomingNodeArray);
					} else {
						tmpJsonObject.put("incomingNode", null);
					}

					_jsonArray.add(tmpJsonObject);
				}
			} catch (ParseException e) {
				// iterable.first().toJson() 이 json형식의 string이 아닌 경우
				// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
				error.put("error", "stagingBolt/2");
				LOG.debug("error : stagingBolt/2");
				_jsonArray.add(error);
				// collector.emit(new Values(_jsonArray));
				return;
			}
		} else if (_jsonObject.get("spoutName").equals("order")) {
			try {
				roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

				mapIds = (JSONObject) roadMapId.get("mapIds");
				initNodeArray = (JSONArray) roadMapId.get("initNode");
				incomingNode = (JSONObject) roadMapId.get("incomingNode");
				outingNode = (JSONObject) roadMapId.get("outingNode");
				lastNodeArray = (JSONArray) roadMapId.get("lastNode");

				String jsonString = _jsonObject.toJSONString();

				for (int i = 0; i < initNodeArray.size(); i++) {
					String InitMapId = (String) initNodeArray.get(i);

					mapId = (JSONObject) mapIds.get(InitMapId);
					JSONObject tmpJsonObject = new JSONObject();

					if (_jsonObject.get("deviceId").equals(mapId.get("deviceId"))) {
						tmpJsonObject = (JSONObject) parser.parse(jsonString);
						tmpJsonObject.put("previousData", null);
						tmpJsonObject.put("order", true);
						tmpJsonObject.put("mapId", InitMapId);
						tmpJsonObject.put("topic",
								tmpJsonObject.get("corporationName") + "/" + tmpJsonObject.get("serverId") + "/"
										+ tmpJsonObject.get("brokerId") + "/" + mapId.get("deviceId"));

						tmpJsonObject.remove("corporationName");
						tmpJsonObject.remove("serverId");
						tmpJsonObject.remove("brokerId");
						tmpJsonObject.remove("spoutName");

						if (outingNode.containsKey(InitMapId)) {
							outingNodeArray = (JSONArray) outingNode.get(InitMapId);

							tmpJsonObject.put("outingNode", outingNodeArray);
							tmpJsonObject.put("lastNode", false);
						} else {
							tmpJsonObject.put("outingNode", null);
							tmpJsonObject.put("lastNode", true);
						}

						if (incomingNode.containsKey(InitMapId)) {
							incomingNodeArray = (JSONArray) incomingNode.get(InitMapId);

							tmpJsonObject.put("incomingNode", incomingNodeArray);
						} else {
							tmpJsonObject.put("incomingNode", null);
						}

						_jsonArray.add(tmpJsonObject);
					} else {

					}
				}
			} catch (ParseException e) {
				// iterable.first().toJson() 이 json형식의 string이 아닌 경우
				// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
				error.put("error", "stagingBolt/3");
				LOG.debug("error : stagingBolt/3");
				_jsonArray.add(error);
				// collector.emit(new Values(_jsonArray));
				return;
			}
		} else if (_jsonObject.get("spoutName").equals("proceed")) {
			try {
				roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

				mapIds = (JSONObject) roadMapId.get("mapIds");
				initNodeArray = (JSONArray) roadMapId.get("initNode");
				incomingNode = (JSONObject) roadMapId.get("incomingNode");
				outingNode = (JSONObject) roadMapId.get("outingNode");
				lastNodeArray = (JSONArray) roadMapId.get("lastNode");

				String currentMapId = (String) _jsonObject.get("mapId");

				mapId = (JSONObject) mapIds.get(currentMapId);

				_jsonObject.put("topic", "enow" + "/" + mapId.get("serverId") + "/" + mapId.get("brokerId") + "/" + mapId.get("deviceId"));

				_jsonObject.remove("spoutName");

				if ((boolean)_jsonObject.get("order")) {
					_jsonObject.put("order",false);
				} else {
				}
				if (outingNode.containsKey(currentMapId)) {
					outingNodeArray = (JSONArray) outingNode.get(currentMapId);

					_jsonObject.put("outingNode", outingNodeArray);
					_jsonObject.put("lastNode", false);
				} else {
					_jsonObject.put("outingNode", null);
					_jsonObject.put("lastNode", true);
				}

				if (incomingNode.containsKey(currentMapId)) {
					incomingNodeArray = (JSONArray) incomingNode.get(currentMapId);

					_jsonObject.put("incomingNode", incomingNodeArray);
				} else {
					_jsonObject.put("incomingNode", null);
				}

				_jsonArray.add(_jsonObject);
			} catch (ParseException e) {
				// iterable.first().toJson() 이 json형식의 string이 아닌 경우
				// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
				error.put("error", "stagingBolt/4");
				LOG.debug("error : stagingBolt/4");
				_jsonArray.add(error);
				// collector.emit(new Values(_jsonArray));
				return;

			}
		} else {
			error.put("error", "stagingBolt/5");
			LOG.debug("error : stagingBolt/5");
			_jsonArray.add(error);
			// collector.emit(new Values(_jsonArray));
			return;
		}

		/*
		 * if ((boolean) _jsonObject.get("init")) { try { roadMapId =
		 * (JSONObject) jsonParser.parse(iterable.first().toJson());
		 * 
		 * mapIds = (JSONObject) roadMapId.get("mapIds"); initNodeArray =
		 * (JSONArray) roadMapId.get("initNode"); incomingNode = (JSONObject)
		 * roadMapId.get("incomingNode"); outingNode = (JSONObject)
		 * roadMapId.get("outingNode"); lastNodeArray = (JSONArray)
		 * roadMapId.get("lastNode");
		 * 
		 * String jsonString = _jsonObject.toJSONString();
		 * 
		 * for (int i = 0; i < initNodeArray.size(); i++) { String InitMapId =
		 * (String) initNodeArray.get(i);
		 * 
		 * mapId = (JSONObject) mapIds.get(InitMapId);
		 * 
		 * JSONObject tmpJsonObject = new JSONObject();
		 * 
		 * tmpJsonObject = (JSONObject) parser.parse(jsonString);
		 * tmpJsonObject.put("payload", null); tmpJsonObject.put("previousData",
		 * null); tmpJsonObject.put("proceed", false);
		 * tmpJsonObject.put("mapId", InitMapId); tmpJsonObject.put("deviceId",
		 * mapId.get("deviceId")); tmpJsonObject.put("topic",
		 * tmpJsonObject.get("corporationName") + "/" +
		 * tmpJsonObject.get("serverId") + "/" + tmpJsonObject.get("brokerId") +
		 * "/" + mapId.get("deviceId"));
		 * 
		 * tmpJsonObject.remove("corporationName");
		 * tmpJsonObject.remove("serverId"); tmpJsonObject.remove("brokerId");
		 * 
		 * if (outingNode.containsKey(InitMapId)) { outingNodeArray =
		 * (JSONArray) outingNode.get(InitMapId);
		 * 
		 * tmpJsonObject.put("outingNode", outingNodeArray);
		 * tmpJsonObject.put("lastNode", false); } else {
		 * tmpJsonObject.put("outingNode", null); tmpJsonObject.put("lastNode",
		 * true); }
		 * 
		 * if (incomingNode.containsKey(InitMapId)) { incomingNodeArray =
		 * (JSONArray) incomingNode.get(InitMapId);
		 * 
		 * tmpJsonObject.put("incomingNode", incomingNodeArray); } else {
		 * tmpJsonObject.put("incomingNode", null); }
		 * 
		 * tmpJsonObject.put("init", false); _jsonArray.add(tmpJsonObject); } }
		 * catch (ParseException e) { // iterable.first().toJson() 이 json형식의
		 * string이 아닌 경우 // 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
		 * e.printStackTrace(); error.put("error", "stagingBolt/1");
		 * _jsonArray.add(error); collector.emit(new Values(_jsonArray)); } }
		 * else { try { roadMapId = (JSONObject)
		 * jsonParser.parse(iterable.first().toJson());
		 * 
		 * mapIds = (JSONObject) roadMapId.get("mapIds"); initNodeArray =
		 * (JSONArray) roadMapId.get("initNode"); incomingNode = (JSONObject)
		 * roadMapId.get("incomingNode"); outingNode = (JSONObject)
		 * roadMapId.get("outingNode"); lastNodeArray = (JSONArray)
		 * roadMapId.get("lastNode");
		 * 
		 * String currentMapId = (String) _jsonObject.get("mapId"); String[]
		 * topic = _jsonObject.get("topic").toString().split("\\/");
		 * 
		 * mapId = (JSONObject) mapIds.get(currentMapId);
		 * 
		 * _jsonObject.put("proceed", false);
		 * 
		 * _jsonObject.put("topic", topic[0] + "/" + topic[1] + "/" + topic[2] +
		 * "/" + mapId.get("deviceId"));
		 * 
		 * if (outingNode.containsKey(currentMapId)) { outingNodeArray =
		 * (JSONArray) outingNode.get(currentMapId);
		 * 
		 * _jsonObject.put("outingNode", outingNodeArray);
		 * _jsonObject.put("lastNode", false); } else {
		 * _jsonObject.put("outingNode", null); _jsonObject.put("lastNode",
		 * true); }
		 * 
		 * if (incomingNode.containsKey(currentMapId)) { incomingNodeArray =
		 * (JSONArray) incomingNode.get(currentMapId);
		 * 
		 * _jsonObject.put("incomingNode", incomingNodeArray); } else {
		 * _jsonObject.put("incomingNode", null); }
		 * 
		 * _jsonArray.add(_jsonObject); } catch (ParseException e) { //
		 * iterable.first().toJson() 이 json형식의 string이 아닌 경우 // 발생 하지만 tojson이기에
		 * 그럴 일이 발생하지 않을 것이라 가정 e.printStackTrace(); error.put("error",
		 * "stagingBolt/2"); _jsonArray.add(error); collector.emit(new
		 * Values(_jsonArray));
		 * 
		 * } }
		 */
		collector.emit(new Values(_jsonArray));

		try {
			LOG.debug("StagingBolt result = [" + _jsonArray.toString() + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonArray"));
	}
}
