package com.enow.storm.TriggerTopology;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import com.google.common.collect.Lists;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.enow.storm.Connect;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StagingBolt2 extends BaseRichBolt {
	protected static final Logger LOG = LogManager.getLogger(StagingBolt2.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		JSONObject _jsonObject = null;
		boolean deviceIdCheck = false;
		boolean phaseRoadMapIdCheck = false;
		boolean mapIdCheck = false;
		boolean brokerIdCheck = false;
		boolean serverIdCheck = false;
		FindIterable<Document> iterable;
		JSONParser jsonParser = new JSONParser();
		JSONObject phaseRoadMapId;
		JSONObject phases;
		JSONObject phaseId;
		JSONObject devices;
		JSONObject incomingPeer;
		JSONObject outingPeer;
		JSONObject waitingPeer;
		JSONObject subsequentInitPeer;
		JSONArray deviceId;
		JSONArray waitingPeerPhase;
		JSONArray subsequentInitPeerPhase;
		JSONArray outingPeerArray;
		JSONArray incomingPeerArray;
		JSONParser parser = new JSONParser();
		ConcurrentHashMap<String, JSONObject> ackSchdueling = new ConcurrentHashMap<>();
		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);

		JSONObject json = null;
		String webhook = null;
		Connect con = new Connect("https://hooks.slack.com/services/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");
		// json = new JSONObject();
		// json.put("text", "2");
		// webhook = con.post(con.getURL(), json);

		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();

		_jsonObject = (JSONObject) input.getValueByField("jsonObject");

		if ((boolean) _jsonObject.get("ack")) {
			if (ackSchdueling.containsKey(_jsonObject.get("mapId"))) {
				if (ackSchdueling.get("mapId") == _jsonObject) {
					ackSchdueling.remove("mapId");
					_jsonArray.add(_jsonObject);
					collector.emit(new Values(_jsonArray, serverIdCheck, brokerIdCheck, deviceIdCheck,
							phaseRoadMapIdCheck, mapIdCheck));
				} else {
					// mapId만 똑같고 들어 있는 값이 달러?!! 들어 오자마자 장난이냐??
					return;
				}
			} else {
				// excute cycle을 돌지도 아났는데 ack를 보내고 있어. 그람 안되는거 안배웠으??
				return;
			}
		} else {
			if ((boolean) _jsonObject.get("init")) {
				// Connect MongoDB
				// Get enow database
				MongoDatabase dbWrite = mongoClient.getDatabase("lists");

				MongoCollection<Document> serverListCollection = dbWrite.getCollection("server");

				if (serverListCollection.count(new Document("serverId", (String) _jsonObject.get("serverId"))) == 0) {
					// There isn't deviceId that matches input signal from
					// device
					serverIdCheck = false;
				} else if (serverListCollection
						.count(new Document("serverId", (String) _jsonObject.get("serverId"))) == 1) {
					// There is deviceId that matches input signal from device
					serverIdCheck = true;
				} else {
					LOG.debug("There are more than two server ID on MongoDB");
					// this should not happen it's our mistake
				}

				// Get device collection for matching input signal from device
				MongoCollection<Document> brokerListCollection = dbWrite.getCollection("broker");

				if (brokerListCollection.count(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 0) {
					// There isn't deviceId that matches input signal from
					// device
					brokerIdCheck = false;
				} else if (brokerListCollection
						.count(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 1) {
					// There is deviceId that matches input signal from device
					brokerIdCheck = true;

				} else {
					// machineIdCheck = "device id : now we have a problem";
					LOG.debug("There are more than two broker ID on MongoDB");
				}

				// Get device collection for matching input signal from device
				MongoCollection<Document> deviceListCollection = dbWrite.getCollection("device");

				if (deviceListCollection.count(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 0) {
					// There isn't deviceId that matches input signal from
					// device
					deviceIdCheck = false;
				} else if (deviceListCollection
						.count(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 1) {
					// There is deviceId that matches input signal from device
					deviceIdCheck = true;
				} else {
					// machineIdCheck = "device id : now we have a problem";
					LOG.debug("There are more than two machine ID on MongoDB");
				}
				// Check Phase Road-map ID

				dbWrite = mongoClient.getDatabase("enow");

				MongoCollection<Document> phaseRoadMapCollection = dbWrite.getCollection("phaseRoadMap");

				if (phaseRoadMapCollection
						.count(new Document("phaseRoadMapId", (String) _jsonObject.get("phaseRoadMapId"))) == 0) {
					// There isn't phaseRoadMapId that matches input signal from
					// device
					phaseRoadMapIdCheck = false;
				} else if (phaseRoadMapCollection
						.count(new Document("phaseRoadMapId", (String) _jsonObject.get("phaseRoadMapId"))) == 1) {
					// There is phaseRoadMapId that matches input signal from
					// device
					phaseRoadMapIdCheck = true;

				} else {
					// phaseRoadMapIdCheck = "phase road map id : now we have a
					// problem";
					LOG.debug("There are more than two Phase Road-map Id on MongoDB");
				}

				if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck) {
					try {
						iterable = phaseRoadMapCollection
								.find(new Document("phaseRoadMapId", (String) _jsonObject.get("phaseRoadMapId")));

						try {
							phaseRoadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
							phases = (JSONObject) phaseRoadMapId.get("phases");
							phaseId = (JSONObject) phases.get("phase1");
							devices = (JSONObject) phaseId.get("devices");
							deviceId = (JSONArray) devices.get((String) _jsonObject.get("deviceId"));

							_jsonObject.put("phaseId", "phase1");
							_jsonObject.put("init", false);
							_jsonObject.put("previousData", null);

							waitingPeer = (JSONObject) phaseRoadMapId.get("waitingPeer");
							subsequentInitPeer = (JSONObject) phaseRoadMapId.get("subsequentInitPeer");

							incomingPeer = (JSONObject) phaseRoadMapId.get("incomingPeer");
							outingPeer = (JSONObject) phaseRoadMapId.get("outingPeer");

							waitingPeerPhase = (JSONArray) waitingPeer.get("phase1");
							subsequentInitPeerPhase = (JSONArray) subsequentInitPeer.get("phase2");

							for (int n = 0; n < deviceId.size(); n++) {
								JSONObject deviceIdList = (JSONObject) deviceId.get(n);
								String jsonString = _jsonObject.toJSONString();

								if (deviceIdList.get("mapId").toString().equals("1")) {
									mapIdCheck = true;

									JSONObject tmpJsonObject = new JSONObject();
									JSONArray tmpJsonArray = new JSONArray();
									tmpJsonObject = (JSONObject) parser.parse(jsonString);
									tmpJsonObject.put("mapId", "1");

									tmpJsonObject.put("incomimgPeer", null);

									if (outingPeer.containsKey("1")) {
										outingPeerArray = (JSONArray) outingPeer.get("1");

										tmpJsonObject.put("outingPeer", outingPeerArray);
									}

									if (waitingPeerPhase.contains("1")) {
										tmpJsonObject.put("waitingPeer", waitingPeerPhase);
										tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
									} else {
										tmpJsonObject.put("waitingPeer", null);
										tmpJsonObject.put("subsequentInitPeer", null);
									}

									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (deviceIdList.get("mapId").toString().equals("3")) {
									mapIdCheck = true;

									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(jsonString);
									tmpJsonObject.put("mapId", "3");

									tmpJsonObject.put("incomimgPeer", null);

									if (outingPeer.containsKey("3")) {
										outingPeerArray = (JSONArray) outingPeer.get("3");

										tmpJsonObject.put("outingPeer", outingPeerArray);
									}

									if (waitingPeerPhase.contains("3")) {
										tmpJsonObject.put("waitingPeer", waitingPeerPhase);
										tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
									} else {
										tmpJsonObject.put("waitingPeer", null);
										tmpJsonObject.put("subsequentInitPeer", null);
									}

									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (deviceIdList.get("mapId").toString().equals("5")) {
									mapIdCheck = true;

									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(jsonString);
									tmpJsonObject.put("mapId", "5");

									tmpJsonObject.put("incomimgPeer", null);

									if (outingPeer.containsKey("5")) {
										outingPeerArray = (JSONArray) outingPeer.get("5");

										tmpJsonObject.put("outingPeer", outingPeerArray);
									}

									if (waitingPeerPhase.contains("5")) {
										tmpJsonObject.put("waitingPeer", waitingPeerPhase);
										tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
									} else {
										tmpJsonObject.put("waitingPeer", null);
										tmpJsonObject.put("subsequentInitPeer", null);
									}

									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (deviceIdList.get("mapId").toString().equals("7")) {
									mapIdCheck = true;

									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(jsonString);
									tmpJsonObject.put("mapId", "7");

									tmpJsonObject.put("incomimgPeer", null);

									if (outingPeer.containsKey("7")) {
										outingPeerArray = (JSONArray) outingPeer.get("7");

										tmpJsonObject.put("outingPeer", outingPeerArray);
									}

									if (waitingPeerPhase.contains("7")) {
										tmpJsonObject.put("waitingPeer", waitingPeerPhase);
										tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
									} else {
										tmpJsonObject.put("waitingPeer", null);
										tmpJsonObject.put("subsequentInitPeer", null);
									}

									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (deviceIdList.get("mapId").toString().equals("9")) {
									mapIdCheck = true;

									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(jsonString);
									tmpJsonObject.put("mapId", "9");

									tmpJsonObject.put("incomimgPeer", null);

									if (outingPeer.containsKey("9")) {
										outingPeerArray = (JSONArray) outingPeer.get("9");

										tmpJsonObject.put("outingPeer", outingPeerArray);
									}

									if (waitingPeerPhase.contains("9")) {
										tmpJsonObject.put("waitingPeer", waitingPeerPhase);
										tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
									} else {
										tmpJsonObject.put("waitingPeer", null);
										tmpJsonObject.put("subsequentInitPeer", null);
									}

									_jsonArray.add(tmpJsonObject);
								} else {
								}
								
								for (int i = 0; i < _jsonArray.size(); i++) {
									JSONObject tmpJsonObject = _jsonArray.get(i);
									if (ackSchdueling.containsKey(tmpJsonObject.get("phaseRoadMapId")+"/"+tmpJsonObject.get("mapId"))) {
										// 난 아직 넣지도 않았는데 이미 mapId가 존재 한대.. 므여
										return;
									} else {
										ackSchdueling.put(tmpJsonObject.get("phaseRoadMapId")+"/"+tmpJsonObject.get("mapId"), tmpJsonObject);
									}
								}
							}
						} catch (ParseException e) {
							// iterable.first().toJson() 이 json형식의 string이 아닌 경우
							// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
							e.printStackTrace();
							mapIdCheck = false;
						}
					} catch (NullPointerException e) {
						e.getMessage();
						mapIdCheck = false;
						// _jsonObject.get("deviceId")가 phase1에 있는 deviceId 중 없는
						// 경우
					}
				} else {
					// serverIdCheck,brokerIdCheck,deviceIdCheck,phaseRoadMapIdCheck
					// 중에 false값이 존재하는 상태.
				}
			} else {
				
				if(ackSchdueling.containsKey(_jsonObject.get("phaseRoadMapId")+"/"+_jsonObject.get("mapId"))){
					
				}else{
					//전에 받은 값이
					return;
				}
				
				serverIdCheck = true;
				brokerIdCheck = true;
				deviceIdCheck = true;
				phaseRoadMapIdCheck = true;
				
				try {
					MongoDatabase dbWrite = mongoClient.getDatabase("enow");

					MongoCollection<Document> phaseRoadMapCollection = dbWrite.getCollection("phaseRoadMap");

					iterable = phaseRoadMapCollection
							.find(new Document("phaseRoadMapId", (String) _jsonObject.get("phaseRoadMapId")));

					try {
						phaseRoadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
						phases = (JSONObject) phaseRoadMapId.get("phases");
						phaseId = (JSONObject) phases.get(_jsonObject.get("phaseId"));
						devices = (JSONObject) phaseId.get("devices");
						deviceId = (JSONArray) devices.get((String) _jsonObject.get("deviceId"));

						_jsonObject.put("phaseId", _jsonObject.get("phaseId"));

						waitingPeer = (JSONObject) phaseRoadMapId.get("waitingPeer");
						subsequentInitPeer = (JSONObject) phaseRoadMapId.get("subsequentInitPeer");

						incomingPeer = (JSONObject) phaseRoadMapId.get("incomingPeer");
						outingPeer = (JSONObject) phaseRoadMapId.get("outingPeer");

						waitingPeerPhase = (JSONArray) waitingPeer.get(_jsonObject.get("phaseId"));

						String tmpString = (String) _jsonObject.get("phaseId");

						if (subsequentInitPeer.containsKey(tmpString.replace(tmpString.substring(5, 6),
								String.valueOf(Character.getNumericValue(tmpString.charAt(5)) + 1)))) {
							subsequentInitPeerPhase = (JSONArray) subsequentInitPeer
									.get(tmpString.replace(tmpString.substring(5, 6),
											String.valueOf(Character.getNumericValue(tmpString.charAt(5)) + 1)));
						} else {
							subsequentInitPeerPhase = null;
						}

						for (int n = 0; n < deviceId.size(); n++) {
							JSONObject deviceIdList = (JSONObject) deviceId.get(n);
							String jsonString = _jsonObject.toJSONString();

							if (deviceIdList.get("mapId").toString().equals((String) _jsonObject.get("mapId"))) {
								mapIdCheck = true;

								JSONObject tmpJsonObject = new JSONObject();
								JSONArray tmpJsonArray = new JSONArray();
								tmpJsonObject = (JSONObject) parser.parse(jsonString);

								if (waitingPeer.containsKey((String) tmpJsonObject.get("mapId"))) {
									incomingPeerArray = (JSONArray) incomingPeer
											.get((String) tmpJsonObject.get("mapId"));

									tmpJsonObject.put("incomingPeer", incomingPeerArray);
								} else {
									tmpJsonObject.put("incomimgPeer", null);
								}

								if (outingPeer.containsKey((String) tmpJsonObject.get("mapId"))) {
									outingPeerArray = (JSONArray) outingPeer.get((String) tmpJsonObject.get("mapId"));

									tmpJsonObject.put("outingPeer", outingPeerArray);
								} else {
									tmpJsonObject.put("outingPeer", null);
								}

								if (waitingPeerPhase.contains((String) tmpJsonObject.get("mapId"))) {
									tmpJsonObject.put("waitingPeer", waitingPeerPhase);
									tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase);
								} else {
									tmpJsonObject.put("waitingPeer", null);
									tmpJsonObject.put("subsequentInitPeer", null);
								}

								_jsonArray.add(tmpJsonObject);
							} else {
							}
						}
					} catch (ParseException e) {
						// iterable.first().toJson() 이 json형식의 string이 아닌 경우
						// 발생 하지만 tojson이기에 그럴 일이 발생하지 않을 것이라 가정
						e.printStackTrace();
						mapIdCheck = false;
					}
				} catch (NullPointerException e) {
					e.getMessage();
					mapIdCheck = false;
					// _jsonObject.get("deviceId")가 phase1에 있는 deviceId 중 없는
					// 경우
				}
			}

			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck) {
				for (int i = 0; i < _jsonArray.size(); i++) {
					JSONObject tmpJsonObject = _jsonArray.get(i);
					if (ackSchdueling.containsKey(tmpJsonObject.get("mapId"))) {
						// 난 아직 넣지도 않았는데 이미 mapId가 존재 한대.. 므여
						return;
					} else {
						ackSchdueling.put((String) tmpJsonObject.get("mapId"), tmpJsonObject);
					}
				}
			}else{
				// serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck 중에 false가 있어
				// 그람 안돠~
				return;
			}
		}

		collector.emit(new Values(_jsonArray));

		try {
			LOG.debug("input = [" + input + "]");
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

/**********************************************
 * if ((boolean) _jsonObject.get("init")) { // Connect MongoDB // Get enow
 * database MongoDatabase dbWrite = mongoClient.getDatabase("lists");
 * 
 * MongoCollection<Document> serverListCollection =
 * dbWrite.getCollection("server");
 * 
 * if (serverListCollection.count(new Document("serverId", (String)
 * _jsonObject.get("serverId"))) == 0) { // There isn't deviceId that matches
 * input signal from device serverIdCheck = false; } else if
 * (serverListCollection .count(new Document("serverId", (String)
 * _jsonObject.get("serverId"))) == 1) { // There is deviceId that matches input
 * signal from device serverIdCheck = true; } else { LOG.debug("There are more
 * than two server ID on MongoDB"); // this should not happen it's our mistake }
 * 
 * // Get device collection for matching input signal from device
 * MongoCollection<Document> brokerListCollection =
 * dbWrite.getCollection("broker");
 * 
 * if (brokerListCollection.count(new Document("brokerId", (String)
 * _jsonObject.get("brokerId"))) == 0) { // There isn't deviceId that matches
 * input signal from device brokerIdCheck = false; } else if
 * (brokerListCollection .count(new Document("brokerId", (String)
 * _jsonObject.get("brokerId"))) == 1) { // There is deviceId that matches input
 * signal from device brokerIdCheck = true;
 * 
 * } else { // machineIdCheck = "device id : now we have a problem";
 * LOG.debug("There are more than two broker ID on MongoDB"); }
 * 
 * // Get device collection for matching input signal from device
 * MongoCollection<Document> deviceListCollection =
 * dbWrite.getCollection("device");
 * 
 * if (deviceListCollection.count(new Document("deviceId", (String)
 * _jsonObject.get("deviceId"))) == 0) { // There isn't deviceId that matches
 * input signal from device deviceIdCheck = false; } else if
 * (deviceListCollection .count(new Document("deviceId", (String)
 * _jsonObject.get("deviceId"))) == 1) { // There is deviceId that matches input
 * signal from device deviceIdCheck = true; } else { // machineIdCheck = "device
 * id : now we have a problem"; LOG.debug("There are more than two machine ID on
 * MongoDB"); } // Check Phase Road-map ID
 * 
 * dbWrite = mongoClient.getDatabase("enow");
 * 
 * MongoCollection<Document> phaseRoadMapCollection =
 * dbWrite.getCollection("phaseRoadMap");
 * 
 * if (phaseRoadMapCollection .count(new Document("phaseRoadMapId", (String)
 * _jsonObject.get("phaseRoadMapId"))) == 0) { // There isn't phaseRoadMapId
 * that matches input signal from // device phaseRoadMapIdCheck = false; } else
 * if (phaseRoadMapCollection .count(new Document("phaseRoadMapId", (String)
 * _jsonObject.get("phaseRoadMapId"))) == 1) { // There is phaseRoadMapId that
 * matches input signal from device phaseRoadMapIdCheck = true;
 * 
 * } else { // phaseRoadMapIdCheck = "phase road map id : now we have a //
 * problem"; LOG.debug("There are more than two Phase Road-map Id on MongoDB");
 * }
 * 
 * if (!(boolean) _jsonObject.get("ack")) { if (serverIdCheck && brokerIdCheck
 * && deviceIdCheck && phaseRoadMapIdCheck) { try { iterable =
 * phaseRoadMapCollection .find(new Document("phaseRoadMapId", (String)
 * _jsonObject.get("phaseRoadMapId")));
 * 
 * try { phaseRoadMapId = (JSONObject)
 * jsonParser.parse(iterable.first().toJson()); phases = (JSONObject)
 * phaseRoadMapId.get("phases"); phaseId = (JSONObject) phases.get("phase1");
 * devices = (JSONObject) phaseId.get("devices"); deviceId = (JSONArray)
 * devices.get((String) _jsonObject.get("deviceId"));
 * 
 * _jsonObject.put("phaseId", "phase1"); _jsonObject.put("init", false);
 * _jsonObject.put("previousData", null);
 * 
 * waitingPeer = (JSONObject) phaseRoadMapId.get("waitingPeer");
 * subsequentInitPeer = (JSONObject) phaseRoadMapId.get("subsequentInitPeer");
 * 
 * incomingPeer = (JSONObject) phaseRoadMapId.get("incomingPeer"); outingPeer =
 * (JSONObject) phaseRoadMapId.get("outingPeer");
 * 
 * waitingPeerPhase = (JSONArray) waitingPeer.get("phase1");
 * subsequentInitPeerPhase = (JSONArray) subsequentInitPeer.get("phase2");
 * 
 * for (int n = 0; n < deviceId.size(); n++) { JSONObject deviceIdList =
 * (JSONObject) deviceId.get(n); String jsonString = _jsonObject.toJSONString();
 * 
 * if (deviceIdList.get("mapId").toString().equals("1")) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); JSONArray tmpJsonArray = new
 * JSONArray(); tmpJsonObject = (JSONObject) parser.parse(jsonString);
 * tmpJsonObject.put("mapId", "1");
 * 
 * tmpJsonObject.put("incomimgPeer", null);
 * 
 * if (outingPeer.containsKey("1")) { outingPeerArray = (JSONArray)
 * outingPeer.get("1");
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); }
 * 
 * if (waitingPeerPhase.contains("1")) { tmpJsonObject.put("waitingPeer",
 * waitingPeerPhase); tmpJsonObject.put("subsequentInitPeer",
 * subsequentInitPeerPhase); } else { tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { }
 * 
 * if (deviceIdList.get("mapId").toString().equals("3")) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); tmpJsonObject = (JSONObject)
 * parser.parse(jsonString); tmpJsonObject.put("mapId", "3");
 * 
 * tmpJsonObject.put("incomimgPeer", null);
 * 
 * if (outingPeer.containsKey("3")) { outingPeerArray = (JSONArray)
 * outingPeer.get("3");
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); }
 * 
 * if (waitingPeerPhase.contains("3")) { tmpJsonObject.put("waitingPeer",
 * waitingPeerPhase); tmpJsonObject.put("subsequentInitPeer",
 * subsequentInitPeerPhase); } else { tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { }
 * 
 * if (deviceIdList.get("mapId").toString().equals("5")) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); tmpJsonObject = (JSONObject)
 * parser.parse(jsonString); tmpJsonObject.put("mapId", "5");
 * 
 * tmpJsonObject.put("incomimgPeer", null);
 * 
 * if (outingPeer.containsKey("5")) { outingPeerArray = (JSONArray)
 * outingPeer.get("5");
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); }
 * 
 * if (waitingPeerPhase.contains("5")) { tmpJsonObject.put("waitingPeer",
 * waitingPeerPhase); tmpJsonObject.put("subsequentInitPeer",
 * subsequentInitPeerPhase); } else { tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { }
 * 
 * if (deviceIdList.get("mapId").toString().equals("7")) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); tmpJsonObject = (JSONObject)
 * parser.parse(jsonString); tmpJsonObject.put("mapId", "7");
 * 
 * tmpJsonObject.put("incomimgPeer", null);
 * 
 * if (outingPeer.containsKey("7")) { outingPeerArray = (JSONArray)
 * outingPeer.get("7");
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); }
 * 
 * if (waitingPeerPhase.contains("7")) { tmpJsonObject.put("waitingPeer",
 * waitingPeerPhase); tmpJsonObject.put("subsequentInitPeer",
 * subsequentInitPeerPhase); } else { tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { }
 * 
 * if (deviceIdList.get("mapId").toString().equals("9")) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); tmpJsonObject = (JSONObject)
 * parser.parse(jsonString); tmpJsonObject.put("mapId", "9");
 * 
 * tmpJsonObject.put("incomimgPeer", null);
 * 
 * if (outingPeer.containsKey("9")) { outingPeerArray = (JSONArray)
 * outingPeer.get("9");
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); }
 * 
 * if (waitingPeerPhase.contains("9")) { tmpJsonObject.put("waitingPeer",
 * waitingPeerPhase); tmpJsonObject.put("subsequentInitPeer",
 * subsequentInitPeerPhase); } else { tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { } } } catch (ParseException e) { //
 * iterable.first().toJson() 이 json형식의 string이 아닌 경우 // 발생 하지만 tojson이기에 그럴 일이
 * 발생하지 않을 것이라 가정 e.printStackTrace(); mapIdCheck = false; } } catch
 * (NullPointerException e) { e.getMessage(); mapIdCheck = false; //
 * _jsonObject.get("deviceId")가 phase1에 있는 deviceId 중 없는 // 경우 } } else { //
 * serverIdCheck,brokerIdCheck,deviceIdCheck,phaseRoadMapIdCheck // 중에 false값이
 * 존재하는 상태. } } else { // init = true인데 ack = true인 상태.console에서 json값을 잘못 보냈다.
 * } } else {///////////////////////////////////// // init = false인 상태 if
 * (!(boolean) _jsonObject.get("ack")) { if (serverIdCheck && brokerIdCheck &&
 * deviceIdCheck && phaseRoadMapIdCheck) { try { MongoDatabase dbWrite =
 * mongoClient.getDatabase("enow");
 * 
 * MongoCollection<Document> phaseRoadMapCollection =
 * dbWrite.getCollection("phaseRoadMap");
 * 
 * iterable = phaseRoadMapCollection .find(new Document("phaseRoadMapId",
 * (String) _jsonObject.get("phaseRoadMapId")));
 * 
 * try { phaseRoadMapId = (JSONObject)
 * jsonParser.parse(iterable.first().toJson()); phases = (JSONObject)
 * phaseRoadMapId.get("phases"); phaseId = (JSONObject)
 * phases.get(_jsonObject.get("phaseId")); devices = (JSONObject)
 * phaseId.get("devices"); deviceId = (JSONArray) devices.get((String)
 * _jsonObject.get("deviceId"));
 * 
 * _jsonObject.put("phaseId", _jsonObject.get("phaseId"));
 * 
 * waitingPeer = (JSONObject) phaseRoadMapId.get("waitingPeer");
 * subsequentInitPeer = (JSONObject) phaseRoadMapId.get("subsequentInitPeer");
 * 
 * incomingPeer = (JSONObject) phaseRoadMapId.get("incomingPeer"); outingPeer =
 * (JSONObject) phaseRoadMapId.get("outingPeer");
 * 
 * waitingPeerPhase = (JSONArray) waitingPeer.get(_jsonObject.get("phaseId"));
 * 
 * String tmpString = (String) _jsonObject.get("phaseId");
 * 
 * if (subsequentInitPeer.containsKey(tmpString.replace(tmpString.substring(5,
 * 6), String.valueOf(Character.getNumericValue(tmpString.charAt(5)) + 1)))) {
 * subsequentInitPeerPhase = (JSONArray) subsequentInitPeer
 * .get(tmpString.replace(tmpString.substring(5, 6),
 * String.valueOf(Character.getNumericValue(tmpString.charAt(5)) + 1))); } else
 * { subsequentInitPeerPhase = null; }
 * 
 * for (int n = 0; n < deviceId.size(); n++) { JSONObject deviceIdList =
 * (JSONObject) deviceId.get(n); String jsonString = _jsonObject.toJSONString();
 * 
 * if (deviceIdList.get("mapId").toString().equals((String)
 * _jsonObject.get("mapId"))) { mapIdCheck = true;
 * 
 * JSONObject tmpJsonObject = new JSONObject(); JSONArray tmpJsonArray = new
 * JSONArray(); tmpJsonObject = (JSONObject) parser.parse(jsonString);
 * 
 * if (waitingPeer.containsKey((String) tmpJsonObject.get("mapId"))) {
 * incomingPeerArray = (JSONArray) incomingPeer .get((String)
 * tmpJsonObject.get("mapId"));
 * 
 * tmpJsonObject.put("incomingPeer", incomingPeerArray); } else {
 * tmpJsonObject.put("incomimgPeer", null); }
 * 
 * if (outingPeer.containsKey((String) tmpJsonObject.get("mapId"))) {
 * outingPeerArray = (JSONArray) outingPeer .get((String)
 * tmpJsonObject.get("mapId"));
 * 
 * tmpJsonObject.put("outingPeer", outingPeerArray); } else {
 * tmpJsonObject.put("outingPeer", null); }
 * 
 * if (waitingPeerPhase.contains((String) tmpJsonObject.get("mapId"))) {
 * tmpJsonObject.put("waitingPeer", waitingPeerPhase);
 * tmpJsonObject.put("subsequentInitPeer", subsequentInitPeerPhase); } else {
 * tmpJsonObject.put("waitingPeer", null);
 * tmpJsonObject.put("subsequentInitPeer", null); }
 * 
 * _jsonArray.add(tmpJsonObject); } else { } } } catch (ParseException e) { //
 * iterable.first().toJson() 이 json형식의 string이 아닌 경우 // 발생 하지만 tojson이기에 그럴 일이
 * 발생하지 않을 것이라 가정 e.printStackTrace(); mapIdCheck = false; } } catch
 * (NullPointerException e) { e.getMessage(); mapIdCheck = false; //
 * _jsonObject.get("deviceId")가 phase1에 있는 deviceId 중 없는 // 경우 } } else { //
 * serverIdCheck,brokerIdCheck,deviceIdCheck,phaseRoadMapIdCheck // 중에 false값이
 * 존재하는 상태. _jsonArray = null; } } else { // ack가 true인 상태로 그냥 지나간다.
 * _jsonArray.add(_jsonObject); } }
 * 
 ********************************/
