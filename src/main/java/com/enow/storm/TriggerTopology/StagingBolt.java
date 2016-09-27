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
	protected static final Logger _LOG = LogManager.getLogger(StagingBolt.class);
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
		JSONObject nodeIds;
		JSONObject nodeId;
		JSONObject outingNode;
		JSONObject incomingNode;
		JSONArray incomingNodeArray;
		JSONArray outingNodeArray;
		JSONArray initNodeArray;
		JSONArray orderNodeArray;
		JSONArray lastNodeArray;
		JSONParser parser = new JSONParser();
		MongoDAO mongoDao;
		JSONObject _jsonError = new JSONObject();
		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();

		_jsonObject = (JSONObject) input.getValueByField("jsonObject");

		if (_jsonObject.containsKey("error")) {
			_LOG.error("error : 1");
			_jsonError.put("error", "error");
			_jsonArray.add(_jsonError);
		} else {
			try {
				// mongoDao = new MongoDAO("192.168.99.100",27017);
				mongoDao = new MongoDAO("127.0.0.1", 27017);

				mongoDao.setDBCollection("enow", "recipes");

				iterable = mongoDao.find(new Document("roadMapId", (String) _jsonObject.get("roadMapId")));

				if (_jsonObject.get("spoutName").equals("event")) {
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						initNodeArray = (JSONArray) roadMapId.get("initNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");

						String jsonString = _jsonObject.toJSONString();

						for (int i = 0; i < initNodeArray.size(); i++) {
							String initNodeId = (String) initNodeArray.get(i);

							nodeId = (JSONObject) nodeIds.get(initNodeId);

							JSONObject tmpJsonObject = new JSONObject();

							tmpJsonObject = (JSONObject) parser.parse(jsonString);
							tmpJsonObject.put("payload", null);
							tmpJsonObject.put("previousData", null);
							tmpJsonObject.put("order", false);
							tmpJsonObject.put("nodeId", initNodeId);
							tmpJsonObject.put("topic", "enow" + "/" + nodeId.get("serverId") + "/"
									+ nodeId.get("brokerId") + "/" + nodeId.get("deviceId"));
							tmpJsonObject.put("verified", true);
							tmpJsonObject.put("lambda", nodeId.get("lambda"));

							tmpJsonObject.remove("spoutName");

							if (outingNode.containsKey(initNodeId)) {
								outingNodeArray = (JSONArray) outingNode.get(initNodeId);

								tmpJsonObject.put("outingNode", outingNodeArray);
								tmpJsonObject.put("lastNode", false);
							} else {
								tmpJsonObject.put("outingNode", null);
								tmpJsonObject.put("lastNode", true);
							}

							if (incomingNode.containsKey(initNodeId)) {
								incomingNodeArray = (JSONArray) incomingNode.get(initNodeId);

								tmpJsonObject.put("incomingNode", incomingNodeArray);
							} else {
								tmpJsonObject.put("incomingNode", null);
							}

							_jsonArray.add(tmpJsonObject);
						}
					} catch (ParseException e) {
						// iterable.first().toJson() 이 json형식의 string이 아닌 경우
						// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
						_LOG.error("error : 2");
						_jsonError.put("error", "error");
						_jsonArray.add(_jsonError);
					}
				} else if (_jsonObject.get("spoutName").equals("order")) {
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						orderNodeArray = (JSONArray) roadMapId.get("orderNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");

						String jsonString = _jsonObject.toJSONString();

						for (int i = 0; i < orderNodeArray.size(); i++) {
							String orderNodeId = (String) orderNodeArray.get(i);

							nodeId = (JSONObject) nodeIds.get(orderNodeId);
							JSONObject tmpJsonObject = new JSONObject();
						
							if (_jsonObject.get("corporationName").equals("enow") && _jsonObject.get("serverId").equals(nodeId.get("serverId")) && _jsonObject.get("brokerId").equals(nodeId.get("brokerId")) && _jsonObject.get("deviceId").equals(nodeId.get("deviceId"))) {
								tmpJsonObject = (JSONObject) parser.parse(jsonString);
								tmpJsonObject.put("previousData", null);
								tmpJsonObject.put("order", true);
								tmpJsonObject.put("nodeId", orderNodeId);
								tmpJsonObject.put("topic",
										tmpJsonObject.get("corporationName") + "/" + tmpJsonObject.get("serverId") + "/"
												+ tmpJsonObject.get("brokerId") + "/" + nodeId.get("deviceId"));
								tmpJsonObject.put("verified", true);
								tmpJsonObject.put("lambda", nodeId.get("lambda"));

								tmpJsonObject.remove("corporationName");
								tmpJsonObject.remove("serverId");
								tmpJsonObject.remove("brokerId");
								tmpJsonObject.remove("spoutName");

								if (outingNode.containsKey(orderNodeId)) {
									outingNodeArray = (JSONArray) outingNode.get(orderNodeId);

									tmpJsonObject.put("outingNode", outingNodeArray);
									tmpJsonObject.put("lastNode", false);
								} else {
									tmpJsonObject.put("outingNode", null);
									tmpJsonObject.put("lastNode", true);
								}

								if (incomingNode.containsKey(orderNodeId)) {
									incomingNodeArray = (JSONArray) incomingNode.get(orderNodeId);

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
						_LOG.error("error : 3");
						_jsonError.put("error", "error");
						_jsonArray.add(_jsonError);
					}
				} else if (_jsonObject.get("spoutName").equals("proceed")) {
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						initNodeArray = (JSONArray) roadMapId.get("initNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");

						String currentNodeId = (String) _jsonObject.get("nodeId");

						nodeId = (JSONObject) nodeIds.get(currentNodeId);

						_jsonObject.put("topic", "enow" + "/" + nodeId.get("serverId") + "/" + nodeId.get("brokerId")
								+ "/" + nodeId.get("deviceId"));
						_jsonObject.put("verified", true);
						_jsonObject.put("lambda", nodeId.get("lambda"));
						_jsonObject.put("order", false);

						_jsonObject.remove("spoutName");

						if (outingNode.containsKey(currentNodeId)) {
							outingNodeArray = (JSONArray) outingNode.get(currentNodeId);

							_jsonObject.put("outingNode", outingNodeArray);
							_jsonObject.put("lastNode", false);
						} else {
							_jsonObject.put("outingNode", null);
							_jsonObject.put("lastNode", true);
						}

						if (incomingNode.containsKey(currentNodeId)) {
							incomingNodeArray = (JSONArray) incomingNode.get(currentNodeId);

							_jsonObject.put("incomingNode", incomingNodeArray);
						} else {
							_jsonObject.put("incomingNode", null);
						}

						_jsonArray.add(_jsonObject);
					} catch (ParseException e) {
						// iterable.first().toJson() 이 json형식의 string이 아닌 경우
						// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
						_LOG.error("error : 4");
						_jsonError.put("error", "error");
						_jsonArray.add(_jsonError);

					}
				} else {
					_LOG.error("error : 5");
					_jsonError.put("error", "error");
					_jsonArray.add(_jsonError);
				}

			} catch (UnknownHostException e) {
				_LOG.error("error : 6");
				_jsonError.put("error", "error");
				_jsonArray.add(_jsonError);
			}
		}

		collector.emit(new Values(_jsonArray));

		try {
			if(_jsonArray.size() ==1 && _jsonArray.get(0).containsKey("error")){
				
			}else{
				for (JSONObject tmp : _jsonArray) {			
					_LOG.info("entered Trigger topology roadMapId : " + tmp.get("roadMapId") + " nodeId : "
							+ tmp.get("nodeId"));
				}
			}
			
			collector.ack(input);
		} catch (Exception e) {
			Log.warn("ack failed");
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonArray"));
	}
}
