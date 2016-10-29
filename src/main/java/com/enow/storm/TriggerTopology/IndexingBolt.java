package com.enow.storm.TriggerTopology;

import java.net.UnknownHostException;
import java.util.Map;

import com.enow.persistence.redis.IRedisDB;
import com.enow.persistence.redis.RedisDB;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.enow.daos.mongoDAO.MongoDAO;
import com.esotericsoftware.minlog.Log;

public class IndexingBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(IndexingBolt.class);
	private OutputCollector collector;
	private MongoDAO mongoDao;
	private String mongoIp;
	private int mongoPort;
	private IRedisDB _redis;
	private String redisIp;
	private int redisPort;
	@Override

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.mongoIp = (String) conf.get("mongodb.ip");
		Long lmongoPort = (Long) conf.get("mongodb.port");
		this.mongoPort = lmongoPort.intValue();
		this.redisIp = (String) conf.get("redis.ip");
		Long lredisPort = (Long) conf.get("redis.port");
        this.redisPort = lredisPort.intValue();
		_redis = RedisDB.getInstance(redisIp, redisPort);
	}

	@Override
	public void execute(Tuple input) {
		JSONParser parser = new JSONParser();
		JSONObject _jsonObject;
		JSONObject _jsonError = new JSONObject();
		JSONObject _jsonStop = new JSONObject();
		boolean corporationNameCheck = false;
		boolean roadMapIdCheck = false;
		boolean brokerIdCheck = false;
		boolean serverIdCheck = false;
		StoppingRoadMap stoppingRoadMap;
		
		if (input.toString().length() == 0) {
			//if input tuple has no value log error : 1
			_LOG.warn("error:1");
			_jsonError.put("error", "true");
			_jsonObject = _jsonError;
		} else {
			//input tuple has a value
			
			String msg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

			try {
				// connecting to MongoDB with docker ip address 192.168.99.100 and port 27017
				// mongoDao = new MongoDAO("192.168.99.100",27017);
				
				// connecting to MongoDB with ip address 127.0.0.1 and port 27017
				mongoDao = new MongoDAO(mongoIp, mongoPort);
				
				if (input.getSourceComponent().equals("event-spout")) {
					//input from event kafka
					
					try {
						_jsonObject = (JSONObject) parser.parse(msg);
						
						if (_jsonObject.containsKey("roadMapId") && _jsonObject.containsKey("status")) {
							//check whether _jsonObject from event kafka has all necessary keys
							if(_jsonObject.get("status").equals("start")){	
								String stoppingRoadMapID = (String)_jsonObject.get("roadMapId");

								if(_redis.isTerminate(stoppingRoadMapID)){
									_jsonStop.put("stop", "true");
									_jsonObject = _jsonStop;
									_LOG.warn("stop:1");
								}else{
									_jsonObject.put("spoutName", "event");
								}
							}else{		
								String stoppingRoadMapID = (String)_jsonObject.get("roadMapId");
								

								_redis.addTerminate(stoppingRoadMapID);

								stoppingRoadMap = new StoppingRoadMap(stoppingRoadMapID, _redis);
								
								stoppingRoadMap.start();
								
								_jsonStop.put("stop", "true");
								_jsonObject = _jsonStop;
								_LOG.warn("stop:1");

							}
						} else {
							//if _jsonObject from event kafka doesn't have all necessary keys log error : 2
							_LOG.warn("error:2");
							_jsonError.put("error", "true");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from event kafka is not a json type log error : 3
						_LOG.warn("error:3");
						_jsonError.put("error", "true");
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
							String stoppingRoadMapID = (String)_jsonObject.get("roadMapId");
							
							if (_redis.isTerminate(stoppingRoadMapID)) {
								_jsonStop.put("stop", "true");
								_jsonObject = _jsonStop;
								_LOG.warn("stop:2");
							} else {
								if (_jsonObject.get("corporationName").equals("enow")) {
									// check whether corporationName is enow
									// for now corporationName must be enow

									corporationNameCheck = true;
								} else {
									corporationNameCheck = false;
								}

								if (_jsonObject.get("serverId").equals("server0")) {
									// check whether serverId is server0
									// for now serverId must be server0
									serverIdCheck = true;
								} else {
									serverIdCheck = false;
								}

								// connecting to brokerList collection in
								// connectionData db
								mongoDao.setDBCollection("connectionData", "brokerList");

								if (mongoDao.collectionCount(
										new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 0) {
									// if MongoDB collection broker has no same
									// value as _jsonObject.get(broker)
									brokerIdCheck = false;
								} else if (mongoDao.collectionCount(
										new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 1) {
									// if MongoDB collection broker has one same
									// value as _jsonObject.get(broker)
									brokerIdCheck = true;
								} else {
									// if MongoDB collection broker has more
									// than two same values as
									// _jsonObject.get(broker)
									brokerIdCheck = false;
									_LOG.debug("There are more than two broker ID on MongoDB");
								}

								// connecting to execute collection in enow db
								mongoDao.setDBCollection("enow", "execute");

								if (mongoDao.collectionCount(
										new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 0) {
									// if MongoDB collection execute in enow db
									// has no same value as
									// _jsonObject.get(device)
									roadMapIdCheck = false;
								} else if (mongoDao.collectionCount(
										new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 1) {
									// if MongoDB collection execute in enow db
									// has one same value as
									// _jsonObject.get(device)
									roadMapIdCheck = true;
								} else {
									// if MongoDB collection execute in enow db
									// has more than two same values as
									// _jsonObject.get(device)
									roadMapIdCheck = false;
									_LOG.debug("There are more than two Phase Road-map Id on MongoDB");
								}

								if (corporationNameCheck && serverIdCheck && brokerIdCheck && roadMapIdCheck) {
									_jsonObject.put("spoutName", "order");
								} else {
									// if more than one out of
									// corporationNameCheck,serverIdCheck,
									// brokerIdCheck, deviceIdCheck, and
									// roadMapIdCheck is false log error : 4
									_LOG.warn("error:4");
									_jsonError.put("error", "true");
									_jsonObject = _jsonError;
								}
							}
						} else {
							//if _jsonObject from order kafka doesn't have all necessary keys log error : 5
							_LOG.warn("error:5");
							_jsonError.put("error", "true");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from order kafka is not a json type log error : 6
						_LOG.warn("error:6");
						_jsonError.put("error", "true");
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
							
							String stoppingRoadMapID = (String)_jsonObject.get("roadMapId");

							if(_redis.isTerminate(stoppingRoadMapID)){
								_jsonStop.put("stop", "true");
								_jsonObject = _jsonStop;
								_LOG.warn("stop:3");
							}else{
								_jsonObject.put("spoutName", "proceed");
							}
						} else {
							//if _jsonObject from proceed kafka doesn't have all necessary keys log error : 7
							_LOG.warn("error:7");
							_jsonError.put("error", "true");
							_jsonObject = _jsonError;
						}
					} catch (ParseException e) {
						//if input tuple from proceed kafka is not a json type log error : 8
						_LOG.warn("error:8");
						_jsonError.put("error", "true");
						_jsonObject = _jsonError;
					}
				} else {
					//if input tuple is not from event kafka or order kafka or proceed kafka log error : 9
					_LOG.warn("error:9");
					_jsonError.put("error", "true");
					_jsonObject = _jsonError;
				}
			} catch (UnknownHostException e) {
				//if MongoDB connection falied log error : 10
				_LOG.warn("error:10");
				_jsonError.put("error", "true");
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

class StoppingRoadMap extends Thread {
	String roadMapId;
	IRedisDB _redis;

	public StoppingRoadMap(String roadMapId, IRedisDB _redis) {
		this.roadMapId = roadMapId;
		this._redis = _redis;
	}

	public void run() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		_redis.deleteTerminate(roadMapId);
		_redis.deleteNode(roadMapId+"-*");

	}
}
