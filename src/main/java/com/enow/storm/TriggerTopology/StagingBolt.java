package com.enow.storm.TriggerTopology;

import java.net.UnknownHostException;
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

import com.enow.daos.mongoDAO.MongoDAO;
import com.enow.storm.Connect;
import com.esotericsoftware.minlog.Log;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class StagingBolt extends BaseRichBolt {
	protected static final Logger LOG = LogManager.getLogger(StagingBolt.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		JSONObject _jsonObject = null;
		FindIterable<Document> iterable;
		JSONParser jsonParser = new JSONParser();
		JSONObject roadMapId;
		JSONObject mapIds;
		JSONObject mapId;
		JSONObject outingNode;
		JSONObject incomingNode;
		JSONArray incomingNodeArray;
		JSONArray outingNodeArray;
		JSONArray initNodeArray;
		JSONArray lastNodeArray;
		JSONParser parser = new JSONParser();
		MongoDAO mongoDao;
		
		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();
		ConcurrentHashMap<String, JSONObject> ackSchdueling = new ConcurrentHashMap<>();
		try {
			//mongoDao = new MongoDAO("192.168.99.100",27017);
			mongoDao = new MongoDAO("127.0.0.1",27017);
		} catch (UnknownHostException e1) {
			LOG.debug("error : 1");
			return;
		}

		_jsonObject = (JSONObject) input.getValueByField("jsonObject");

		mongoDao.setDBCollection("source", "execute");

		iterable = mongoDao.find(new Document("roadMapId", (String) _jsonObject.get("roadMapId")));

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
					tmpJsonObject.put("verified", true);
					tmpJsonObject.put("lambda", mapId.get("lambda"));
					
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
				LOG.debug("error : 2");
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
						tmpJsonObject.put("verified", true);
						tmpJsonObject.put("lambda", mapId.get("lambda"));
						
						
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
				LOG.debug("error : 3");
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
				_jsonObject.put("verified", true);
				_jsonObject.put("lambda", mapId.get("lambda"));
				
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
				LOG.debug("error : 4");
				return;

			}
		} else {
			LOG.debug("error : 5");
			return;
		}

		collector.emit(new Values(_jsonArray));

		try {
			LOG.info(_jsonArray);
			collector.ack(input);
		} catch (Exception e) {
			Log.error("ack failed");
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonArray"));
	}
}
