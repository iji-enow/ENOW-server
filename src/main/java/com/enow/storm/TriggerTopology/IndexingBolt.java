package com.enow.storm.TriggerTopology;

import java.net.UnknownHostException;
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

import com.enow.daos.mongoDAO.MongoDAO;
import com.enow.storm.Connect;
import com.esotericsoftware.minlog.Log;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IndexingBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(IndexingBolt.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		JSONParser parser = new JSONParser();
		JSONObject _jsonObject;
		JSONObject _jsonError = new JSONObject();
		boolean corporationNameCheck = false;
		boolean deviceIdCheck = false;
		boolean roadMapIdCheck = false;
		boolean brokerIdCheck = false;
		boolean serverIdCheck = false;
		MongoDAO mongoDao;
		
		if (input.toString().length() == 0) {
			//if input tuple has no value log error : 1
			_LOG.error("error : 1");
			_jsonError.put("error", "error");
			_jsonObject = _jsonError;
		} else {
			//input tuple has a value
			
			String msg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
			
			
			try {
				// mongoDao = new MongoDAO("192.168.99.100",27017);
				
				//connecting to MongoDB with ip address 127.0.0.1 and port 27017
				mongoDao = new MongoDAO("127.0.0.1", 27017);
				
				if (input.getSourceComponent().equals("event-spout")) {
					//input from event kafka
					
					try {
						_jsonObject = (JSONObject) parser.parse(msg);

						if (_jsonObject.containsKey("roadMapId")) {
							//check whether _jsonObject from event kafka has all necessary keys
							_jsonObject.put("spoutName", "event");
						} else {
							//if _jsonObject from event kafka doesn't have all necessary keys log error : 2
							_LOG.error("error : 2");
							_jsonError.put("error", "error");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from event kafka is not a json type log error : 3
						_LOG.error("error : 3");
						_jsonError.put("error", "error");
						_jsonObject = _jsonError;
					}
				} else if (input.getSourceComponent().equals("order-spout")) {
					//input from order kafka
					
					try {
						_jsonObject = (JSONObject) parser.parse(msg);

						if (_jsonObject.containsKey("corporationName") && _jsonObject.containsKey("serverId")
								&& _jsonObject.containsKey("brokerId") && _jsonObject.containsKey("roadMapId")
								&& _jsonObject.containsKey("deviceId") && _jsonObject.containsKey("payload")) {
							//check whether _jsonObject from order kafka has all necessary keys
									
							if(_jsonObject.get("corporationName").equals("enow")){
								//check whether corporationName is enow
								//for now corporationName must be enow
								
								corporationNameCheck = true;
							}else{
								corporationNameCheck = false;
							}
							
							if(_jsonObject.get("serverId").equals("server0")){
								//check whether serverId is server0
								//for now serverId must be server0
								serverIdCheck = true;
							}else{
								serverIdCheck = false;
							}

							/*
							//connecting to broker collection in lists db
							mongoDao.setDBCollection("lists", "broker");

							if (mongoDao
									.collectionCount(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 0) {
								//if MongoDB collection broker has no same value as _jsonObject.get(broker) 
								brokerIdCheck = false;
							} else if (mongoDao
									.collectionCount(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 1) {
								//if MongoDB collection broker has one same value as _jsonObject.get(broker) 
								brokerIdCheck = true;
							} else {
								//if MongoDB collection broker has more than two same values as _jsonObject.get(broker)
								brokerIdCheck = false;
								_LOG.debug("There are more than two broker ID on MongoDB");
							}

							
							//connecting to device collection in lists db
							mongoDao.setCollection("device");

							if (mongoDao
									.collectionCount(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 0) {
								//if MongoDB collection device has no same value as _jsonObject.get(device) 
								deviceIdCheck = false;
							} else if (mongoDao
									.collectionCount(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 1) {
								//if MongoDB collection device has one same value as _jsonObject.get(device) 
								deviceIdCheck = true;
							} else {
								//if MongoDB collection device has more than two same values as _jsonObject.get(device)
								deviceIdCheck = false;
								_LOG.debug("There are more than two machine ID on MongoDB");
							}
							*/

							//connecting to execute collection in enow db
							mongoDao.setDBCollection("enow", "execute");

							if (mongoDao.collectionCount(
									new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 0) {
								//if MongoDB collection execute in enow db has no same value as _jsonObject.get(device) 
								roadMapIdCheck = false;
							} else if (mongoDao.collectionCount(
									new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 1) {
								//if MongoDB collection execute in enow db has one same value as _jsonObject.get(device) 
								roadMapIdCheck = true;
							} else {
								//if MongoDB collection execute in enow db has more than two same values as _jsonObject.get(device)  
								roadMapIdCheck = false;
								_LOG.debug("There are more than two Phase Road-map Id on MongoDB");
							}

							if (corporationNameCheck && serverIdCheck && brokerIdCheck && deviceIdCheck && roadMapIdCheck) {
								_jsonObject.put("spoutName", "order");
							} else {
								//if more than one out of corporationNameCheck,serverIdCheck, brokerIdCheck, deviceIdCheck, and roadMapIdCheck is false log error : 4
								_LOG.error("error : 4");
								_jsonError.put("error", "error");
								_jsonObject = _jsonError;
							}
						} else {
							//if _jsonObject from order kafka doesn't have all necessary keys log error : 5
							_LOG.error("error : 5");
							_jsonError.put("error", "error");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from order kafka is not a json type log error : 6
						_LOG.error("error : 6");
						_jsonError.put("error", "error");
						_jsonObject = _jsonError;
					}
				} else if (input.getSourceComponent().equals("proceed-spout")) {
					//input from proceed kafka
					
					try {
						_jsonObject = (JSONObject) parser.parse(msg);

						if (_jsonObject.containsKey("order") && _jsonObject.containsKey("roadMapId")
								&& _jsonObject.containsKey("nodeId") && _jsonObject.containsKey("payload")
								&& _jsonObject.containsKey("incomingNode") && _jsonObject.containsKey("outingNode")
								&& _jsonObject.containsKey("previousData") && _jsonObject.containsKey("topic")
								&& _jsonObject.containsKey("lastNode") && _jsonObject.containsKey("verified")
								&& _jsonObject.containsKey("lambda")) {
							//check whether _jsonObject form proceed kafka has all necessary keys

							_jsonObject.put("spoutName", "proceed");
						} else {
							//if _jsonObject from proceed kafka doesn't have all necessary keys log error : 7
							_LOG.error("error : 7");
							_jsonError.put("error", "error");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from proceed kafka is not a json type log error : 8
						_LOG.error("error : 8");
						_jsonError.put("error", "error");
						_jsonObject = _jsonError;
					}
				} else {
					//if input tuple is not from event kafka or order kafka or proceed kafka log error : 9
					_LOG.error("error : 9");
					_jsonError.put("error", "error");
					_jsonObject = _jsonError;
				}
			} catch (UnknownHostException e) {
				//if MongoDB connection falied log error : 10
				_LOG.error("error : 10");
				_jsonError.put("error", "error");
				_jsonObject = _jsonError;
			}
		}

		collector.emit(new Values(_jsonObject));

		try {
			collector.ack(input);
		} catch (Exception e) {
			Log.warn("ack failed");
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonObject"));
	}
}