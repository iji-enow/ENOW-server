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
	private MongoDAO mongoDao;
	private String mongoIp;
	private int mongoPort;

	@Override

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.mongoIp = (String)conf.get("mongodb.ip");
		Long lmongoPort = (Long) conf.get("mongodb.port");
		this.mongoPort = lmongoPort.intValue();
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
		JSONObject _jsonError = new JSONObject();
		JSONObject _jsonStop = new JSONObject();
		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();

		_jsonObject = (JSONObject) input.getValueByField("jsonObject");

		if (_jsonObject.containsKey("error")) {
			//if _jsonObject contains key error it means indexingBolt occured an error log error : 1
			_jsonError.put("error", "true");
			_jsonArray.add(_jsonError);
		}else if(_jsonObject.containsKey("stop")){
			_jsonStop.put("stop", "true");
			_jsonArray.add(_jsonStop);
		}else {
			try {
				// connecting to MongoDB with docker ip address 192.168.99.100 and port 27017
				// mongoDao = new MongoDAO("192.168.99.100",27017);
				
				// connecting to MongoDB with ip address 127.0.0.1 and port 27017
				mongoDao = new MongoDAO("127.0.0.1", 27017);

				// connecting to execute collection in enow db
				mongoDao.setDBCollection("enow", "execute");

				// get document which roadMapId equals to _jsonObject.get("roadMapId")
				iterable = mongoDao.find(new Document("roadMapId", (String) _jsonObject.get("roadMapId")));

				if (_jsonObject.get("spoutName").equals("event")) {
					//input from event kafka
					
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
						
						//get nodeIds,initNode,incomingNode,outingNode,lastNode from document which roadMapId equals to _jsonObject.get("roadMapId")
						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						initNodeArray = (JSONArray) roadMapId.get("initNode"); 
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						

						String jsonString = _jsonObject.toJSONString();

						//repeat running for each initNode in initNodeArray
						for (int i = 0; i < initNodeArray.size(); i++) {
							String initNodeId = (String) initNodeArray.get(i);

							nodeId = (JSONObject) nodeIds.get(initNodeId);

							JSONObject tmpJsonObject = new JSONObject();

							//set appropriate value for necessary keys
							tmpJsonObject = (JSONObject) parser.parse(jsonString);
							tmpJsonObject.put("payload", null);
							tmpJsonObject.put("initNode",true);
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
						//if iterable.first().toJson() is not a json type log error : 2
						//but as you see iterable.first().toJson() is toJson. We suppose that this error won't happen
						_LOG.warn("error:1");
						_jsonError.put("error", "true");
						_jsonArray.add(_jsonError);
					}
				} else if (_jsonObject.get("spoutName").equals("order")) {
					//input from order kafka
					
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

						//get nodeIds,initNode,incomingNode,outingNode,lastNode from document which roadMapId equals to _jsonObject.get("roadMapId")
						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						orderNodeArray = (JSONArray) roadMapId.get("orderNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");

						String jsonString = _jsonObject.toJSONString();

						int count = 0;
						
						//repeat running for each initNode in initNodeArray
						for (int i = 0; i < orderNodeArray.size(); i++) {
							String orderNodeId = (String) orderNodeArray.get(i);
							

							nodeId = (JSONObject) nodeIds.get(orderNodeId);
							JSONObject tmpJsonObject = new JSONObject();
						
							if (_jsonObject.get("corporationName").equals("enow") && _jsonObject.get("serverId").equals("server0") && _jsonObject.get("brokerId").equals(nodeId.get("brokerId")) && _jsonObject.get("deviceId").equals(nodeId.get("deviceId"))) {
								//since order kafka is from client directly check whether corporationName,serverId,brokerId,deviceId all matches to nodeId
								
								//set appropriate value for necessary keys
								tmpJsonObject = (JSONObject) parser.parse(jsonString);
								tmpJsonObject.put("previousData", null);
								tmpJsonObject.put("order", true);
								tmpJsonObject.put("nodeId", orderNodeId);
								tmpJsonObject.put("topic",
										tmpJsonObject.get("corporationName") + "/" + tmpJsonObject.get("serverId") + "/"
												+ tmpJsonObject.get("brokerId") + "/" + nodeId.get("deviceId"));
								tmpJsonObject.put("verified", true);
								tmpJsonObject.put("lambda", nodeId.get("lambda"));
								tmpJsonObject.put("initNode",false);

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
								count++;
							}		
						}
						
						if(count == 0){
							_LOG.warn("error:2");
							_jsonError.put("error", "true");
							_jsonArray.add(_jsonError);
						}
						
					} catch (ParseException e) {
						//if iterable.first().toJson() is not a json type log error : 3
						//but as you see iterable.first().toJson() is toJson. We suppose that this error won't happen
						_LOG.warn("error:3");
						_jsonError.put("error", "true");
						_jsonArray.add(_jsonError);
					}
				} else if (_jsonObject.get("spoutName").equals("proceed")) {
					// input from proceed kafka
					
					try {
						roadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());

						// get nodeIds,initNode,incomingNode,outingNode,lastNode
						// from document which roadMapId equals to _jsonObject.get("roadMapId")
						nodeIds = (JSONObject) roadMapId.get("nodeIds");
						initNodeArray = (JSONArray) roadMapId.get("initNode");
						incomingNode = (JSONObject) roadMapId.get("incomingNode");
						outingNode = (JSONObject) roadMapId.get("outingNode");
						lastNodeArray = (JSONArray) roadMapId.get("lastNode");

						String currentNodeId = (String) _jsonObject.get("nodeId");

						nodeId = (JSONObject) nodeIds.get(currentNodeId);

						_jsonObject.put("topic", "enow/" + nodeId.get("serverId") + "/" + nodeId.get("brokerId")
								+ "/" + nodeId.get("deviceId"));
						_jsonObject.put("verified", true);
						_jsonObject.put("lambda", nodeId.get("lambda"));
						_jsonObject.put("order", false);
						_jsonObject.put("initNode",false);

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
						//if iterable.first().toJson() is not a json type log error : 4
						//but as you see iterable.first().toJson() is toJson. We suppose that this error won't happen
						_LOG.warn("error:4");
						_jsonError.put("error", "true");
						_jsonArray.add(_jsonError);

					}
				} else {
					//if input tuple is not from event kafka or order kafka or proceed kafka log error : 5
					_LOG.warn("error:5");
					_jsonError.put("error", "true");
					_jsonArray.add(_jsonError);
				}
			} catch (UnknownHostException e) {
				//if MongoDB connection falied log error : 6
				_LOG.warn("error:6");
				_jsonError.put("error", "true");
				_jsonArray.add(_jsonError);
			}
		}

		collector.emit(new Values(_jsonArray));

		try {	
			collector.ack(input);
			
			//log entered roadMapId and nodeId if error has not occured
			if(_jsonArray.size() ==1 && _jsonArray.get(0).containsKey("error")){
				
			}else if(_jsonArray.size() ==1 && _jsonArray.get(0).containsKey("stop")){
				
			}else{
				for (JSONObject tmp : _jsonArray) {			
					_LOG.info(tmp.get("roadMapId") + ","+ tmp.get("nodeId") + "|" + tmp.get("topic") +"|" + tmp.toString());
				}
			}
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
