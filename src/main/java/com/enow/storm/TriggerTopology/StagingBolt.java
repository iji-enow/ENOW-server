package com.enow.storm.TriggerTopology;

import java.util.Map;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StagingBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(CallingTriggerBolt.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		TopicStructure _topicStructure = new TopicStructure();
		boolean deviceIdCheck = false;
		boolean phaseRoadMapIdCheck = false;
		boolean mapIdCheck = false;
		boolean brokerIdCheck = false;
		boolean serverIdCheck = false;
		FindIterable<Document> iterable;
		String spoutSource;
		JSONParser jsonParser = new JSONParser();
		JSONObject phaseRoadMapId;
		JSONObject phases;
		JSONObject phaseId;
		JSONObject devices;
		JSONArray deviceId;
		JSONArray ifLastMapId;
		boolean ifLastMapIdCheck = false;
		ArrayList<Integer> waitMapId = new ArrayList<Integer>();
		ArrayList<TopicStructure> topicStructureArray = new ArrayList<TopicStructure>();
		TopicStructure tmpTopicStructure = new TopicStructure();

		_topicStructure = (TopicStructure) input.getValueByField("topicStructure");
		spoutSource = input.getStringByField("spoutSource");
		
	
		
		
		if (_topicStructure.isEmpty()) {
			return;
		} else if (spoutSource == null) {
			return;
		}
		

		// Connect MongoDB
		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		// Get enow database
		MongoDatabase dbWrite = mongoClient.getDatabase("lists");

		MongoCollection<Document> serverListCollection = dbWrite.getCollection("server");

		if (serverListCollection.count(new Document("serverId", _topicStructure.getServerId())) == 0) {
			// There isn't deviceId that matches input signal from device
			serverIdCheck = false;
		} else if (serverListCollection.count(new Document("serverId", _topicStructure.getServerId())) == 1) {
			// There is deviceId that matches input signal from device
			serverIdCheck = true;
		} else {
			LOG.debug("There are more than two server ID on MongoDB");
			// this should not happen it's our mistake
		}

		// Get device collection for matching input signal from device
		MongoCollection<Document> brokerListCollection = dbWrite.getCollection("broker");

		if (brokerListCollection.count(new Document("brokerId", _topicStructure.getBrokerId())) == 0) {
			// There isn't deviceId that matches input signal from device
			brokerIdCheck = false;
		} else if (brokerListCollection.count(new Document("brokerId", _topicStructure.getBrokerId())) == 1) {
			// There is deviceId that matches input signal from device
			brokerIdCheck = true;
		} else {
			// machineIdCheck = "device id : now we have a problem";
			LOG.debug("There are more than two broker ID on MongoDB");
		}

		// Get device collection for matching input signal from device
		MongoCollection<Document> deviceListCollection = dbWrite.getCollection("device");

		if (deviceListCollection.count(new Document("deviceId", _topicStructure.getDeviceId())) == 0) {
			// There isn't deviceId that matches input signal from device
			deviceIdCheck = false;
		} else if (deviceListCollection.count(new Document("deviceId", _topicStructure.getDeviceId())) == 1) {
			// There is deviceId that matches input signal from device
			deviceIdCheck = true;
		} else {
			// machineIdCheck = "device id : now we have a problem";
			LOG.debug("There are more than two machine ID on MongoDB");
		}
		// Check Phase Road-map ID

		dbWrite = mongoClient.getDatabase("enow");

		MongoCollection<Document> phaseRoadMapCollection = dbWrite.getCollection("phaseRoadMap");

		try {
			if (phaseRoadMapCollection.count(
					new Document("phaseRoadMapId", Integer.parseInt(_topicStructure.getPhaseRoadMapId()))) == 0) {
				// There isn't phaseRoadMapId that matches input signal from
				// device
				phaseRoadMapIdCheck = false;
			} else if (phaseRoadMapCollection.count(
					new Document("phaseRoadMapId", Integer.parseInt(_topicStructure.getPhaseRoadMapId()))) == 1) {
				// There is phaseRoadMapId that matches input signal from device
				phaseRoadMapIdCheck = true;
			} else {
				// phaseRoadMapIdCheck = "phase road map id : now we have a
				// problem";
				LOG.debug("There are more than two Phase Road-map Id on MongoDB");
			}
		} catch (NumberFormatException e) {
			e.getMessage();
			phaseRoadMapIdCheck = false;
			// topicStructure.getPhaseRoadMapId()에 숫자가 들어오지 않았을경우
		}
		// If phaseRoadMapIdCheck and machineIdCheck are confirmed
		// insert data to topicStructure

		if (spoutSource.equals("trigger")) {
			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck) {
				try {
					iterable = phaseRoadMapCollection.find(
							new Document("phaseRoadMapId", Integer.parseInt(_topicStructure.getPhaseRoadMapId())));

					try {
						phaseRoadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
						phases = (JSONObject) phaseRoadMapId.get("phases");
						phaseId = (JSONObject) phases.get("phase1");
						devices = (JSONObject) phaseId.get("devices");
						deviceId = (JSONArray) devices.get(_topicStructure.getDeviceId());

						_topicStructure.setPhaseId("phase1");

						ifLastMapId = (JSONArray) phaseRoadMapId.get("phase1");

						for (int i = 0; i < ifLastMapId.size(); i++) {
							Double tmpDouble = (Double)ifLastMapId.get(i);
							waitMapId.add(tmpDouble.intValue());
						}

						for (int n = 0; n < deviceId.size(); n++) {
							JSONObject deviceIdList = (JSONObject) deviceId.get(n);

							try {
								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 1) {
									mapIdCheck = true;

									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("1");

									/*
									if (waitMapId.contains(2)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/
									topicStructureArray.add(tmpTopicStructure);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 3) {
									mapIdCheck = true;
									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("3");

									/*
									if (waitMapId.contains(3)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/

									topicStructureArray.add(tmpTopicStructure);

								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 5) {
									mapIdCheck = true;
									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("5");

									/*
									if (waitMapId.contains(5)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/
									
									topicStructureArray.add(tmpTopicStructure);

								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 7) {
									mapIdCheck = true;
									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("7");

									/*
									if (waitMapId.contains(7)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/

									topicStructureArray.add(tmpTopicStructure);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 9) {
									mapIdCheck = true;
									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("9");

									/*
									if (waitMapId.contains(9)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/

									topicStructureArray.add(tmpTopicStructure);

								} else {
								}
							} catch (NumberFormatException e) {
								// 우리가 mapId에 숫자가 아닌 값을 넣었다. 우리의 잘못.
								mapIdCheck = false;
							}
						}
					} catch (ParseException e) {
						// iterable.first().toJson() 이 json형식의 string이 아닌 경우 발생
						// 하지만
						// tojson이기에 그럴 일은 발생하지 않은 것이라 가정.
						e.printStackTrace();
						mapIdCheck = false;
					}
				} catch (NumberFormatException e) {
					e.getMessage();
					phaseRoadMapIdCheck = false;
					// topicStructure.getPhaseRoadMapId()이 숫자가 들어오지 않았을경우
				} catch (NullPointerException e) {
					e.getMessage();
					mapIdCheck = false;
					// topicStructure.getDeviceId() 가 phase1에 있는 deviceId중 없는 경
				}
			}
		}else if (spoutSource.equals("trigger")) {
			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck) {
				try {
					iterable = phaseRoadMapCollection.find(
							new Document("phaseRoadMapId", Integer.parseInt(_topicStructure.getPhaseRoadMapId())));

					try {
						phaseRoadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
						phases = (JSONObject) phaseRoadMapId.get("phases");
						phaseId = (JSONObject) phases.get(_topicStructure.getPhaseId());
						devices = (JSONObject) phaseId.get("devices");
						deviceId = (JSONArray) devices.get(_topicStructure.getDeviceId());

						_topicStructure.setPhaseId(_topicStructure.getPhaseId());

						ifLastMapId = (JSONArray) phaseRoadMapId.get(_topicStructure.getPhaseId());

						for (int i = 0; i < ifLastMapId.size(); i++) {
							Double tmpDouble = (Double)ifLastMapId.get(i);
							waitMapId.add(tmpDouble.intValue());
						}

						for (int n = 0; n < deviceId.size(); n++) {
							JSONObject deviceIdList = (JSONObject) deviceId.get(n);

							try {
								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 1) {
									mapIdCheck = true;

									tmpTopicStructure = new TopicStructure();
									tmpTopicStructure.setCorporationName(_topicStructure.getCorporationName());
									tmpTopicStructure.setServerId(_topicStructure.getServerId());
									tmpTopicStructure.setBrokerId(_topicStructure.getBrokerId());
									tmpTopicStructure.setPhaseRoadMapId(_topicStructure.getPhaseRoadMapId());
									tmpTopicStructure.setDeviceId(_topicStructure.getDeviceId());
									tmpTopicStructure.setPhaseId(_topicStructure.getPhaseId());
									tmpTopicStructure.setCurrentMsg(_topicStructure.getCurrentMsg());
									tmpTopicStructure.setCurrentMapId("1");

									/*
									if (waitMapId.contains(2)) {
										tmpTopicStructure.setWaitMapId(waitMapId);
										tmpTopicStructure.setLastMapId(true);
									}
									*/

									topicStructureArray.add(tmpTopicStructure);
								} else {
								}
							} catch (NumberFormatException e) {
								// 우리가 mapId에 숫자가 아닌 값을 넣었다. 우리의 잘못.
								mapIdCheck = false;
							}
						}
					} catch (ParseException e) {
						// iterable.first().toJson() 이 json형식의 string이 아닌 경우 발생
						// 하지만
						// tojson이기에 그럴 일은 발생하지 않은 것이라 가정.
						e.printStackTrace();
						mapIdCheck = false;
					}
				} catch (NumberFormatException e) {
					e.getMessage();
					phaseRoadMapIdCheck = false;
					// _topicStructure.getPhaseRoadMapId()이 숫자가 들어오지 않았을경우
				} catch (NullPointerException e) {
					e.getMessage();
					mapIdCheck = false;
					// _topicStructure.getDeviceId() 가 _topicStrcuture.getPhaseId()에 있는 deviceId중 없는 경우 
					// _topicStrcuture.getPhaseId()가 _topicStructure.getPhaseRoadMapId()에 없는 경우 
				}
			}
		}else{
			return;
		}

		/*
		 * if (phaseRoadMapIdCheck && machineIdCheck) { try { iterable =
		 * phaseRoadMapCollection .find(new Document("phaseRoadMapId",
		 * Integer.parseInt(topicStructure.getPhaseRoadMapId())));
		 * 
		 * try { phaseRoadMapId = (JSONObject)
		 * jsonParser.parse(iterable.first().toJson()); phases = (JSONObject)
		 * phaseRoadMapId.get("phases"); phaseId = (JSONObject)
		 * phases.get("phase1"); devices = (JSONObject) phaseId.get("devices");
		 * deviceId = (JSONObject) devices.get(topicStructure.getDeviceId());
		 * 
		 * try { if (Double.parseDouble(deviceId.get("mapId").toString()) == 1)
		 * { mapIdCheck = true; topicStructure.setCurrentMapId("1"); } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 3) {
		 * mapIdCheck = true; topicStructure.setCurrentMapId("3"); } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 5) {
		 * mapIdCheck = true; topicStructure.setCurrentMapId("5"); } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 7) {
		 * mapIdCheck = true; topicStructure.setCurrentMapId("7"); } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 9) {
		 * mapIdCheck = true; topicStructure.setCurrentMapId("9"); } else {
		 * mapIdCheck = false; } } catch (NumberFormatException e) { // 우리가
		 * mapId에 숫자가 아닌 값을 넣었다. 우리의 잘못. mapIdCheck = false; } } catch
		 * (ParseException e) { // iterable.first().toJson() 이 json형식의 string이
		 * 아닌 경우 발생 하지만 // tojson이기에 그럴 일은 발생하지 않은 것이라 가정. e.printStackTrace();
		 * mapIdCheck = false; } } catch (NumberFormatException e) {
		 * e.getMessage(); phaseRoadMapIdCheck = false; //
		 * topicStructure.getPhaseRoadMapId()이 숫자가 들어오지 않았을경우 } catch
		 * (NullPointerException e) { System.out.println(e.getMessage());
		 * mapIdCheck = false; // topicStructure.getDeviceId() 가 phase1에 있는
		 * deviceId중 없는 경 } }
		 */
		/*
		 * try { iterable = phaseRoadMapCollection .find(new
		 * Document("phaseRoadMapId",
		 * Integer.parseInt(topicStructure.getPhaseRoadMapId())));
		 * 
		 * iterable.forEach(new Block<Document>() {
		 * 
		 * @Override public void apply(final Document document) { JSONParser
		 * jsonParser = new JSONParser(); JSONObject phaseRoadMapId; JSONObject
		 * phases; JSONObject phaseId; JSONObject devices; JSONObject deviceId;
		 * JSONObject tmp;
		 * 
		 * try { phaseRoadMapId = (JSONObject)
		 * jsonParser.parse(document.toJson()); phases = (JSONObject)
		 * phaseRoadMapId.get("phases"); phaseId = (JSONObject)
		 * phases.get("phase1"); devices = (JSONObject) phaseId.get("devices");
		 * deviceId = (JSONObject) devices.get(topicStructure.getDeviceId());
		 * 
		 * try { if (Double.parseDouble(deviceId.get("mapId").toString()) == 1)
		 * { mapIdCheck = true; } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 3) {
		 * mapIdCheck = true; } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 5) {
		 * mapIdCheck = true; } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 7) {
		 * mapIdCheck = true; } else if
		 * (Double.parseDouble(deviceId.get("mapId").toString()) == 9) {
		 * mapIdCheck = true; } else { mapIdCheck = false; } } catch
		 * (NumberFormatException e) { // 우리가 mapId에 숫자가 아닌 값을 넣었다. 우리의 잘못.
		 * mapIdCheck = false; }
		 * 
		 * JSONObject json = null; String webhook = null; Connect con = new
		 * Connect(
		 * "https://hooks.slack.com/services/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"
		 * );
		 * 
		 * json = new JSONObject(); json.put("text",
		 * Double.parseDouble(deviceId.get("mapId").toString())); webhook =
		 * con.post(con.getURL(), json);
		 * 
		 * // phaseNum = (JSONObject)phaseId.get("phase1"); // deviceId =
		 * (JSONObject)phaseNum.get("devices");
		 * 
		 * } catch (ParseException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } } }); } catch (NumberFormatException e) {
		 * System.out.println(e.getMessage()); phaseRoadMapIdCheck = false; }
		 * catch (NullPointerException e) { System.out.println(e.getMessage());
		 * mapIdCheck = false; }
		 */

		/*
		 * iterable.forEach(new Block<Document>() {
		 * 
		 * @Override public void apply(final Document document) { JSONParser
		 * jsonParser = new JSONParser(); JSONObject phaseRoadMapId; JSONArray
		 * phases; JSONObject phaseId; JSONObject devices; JSONArray deviceId;
		 * JSONObject tmp;
		 * 
		 * try { // JSON데이터를 넣어 JSON Object 로 만들어 준다. phaseRoadMapId =
		 * (JSONObject) jsonParser.parse(document.toJson());
		 * //jsonObject.get("").toString(); phases =
		 * (JSONArray)phaseRoadMapId.get("phases"); phaseId =
		 * (JSONObject)phases.get(0); //phaseId.get("phase1").toString() devices
		 * = (JSONObject)phaseId.get("phase1"); deviceId =
		 * (JSONArray)devices.get("devices"); tmp = (JSONObject)deviceId.get(0);
		 * 
		 * JSONObject json = null; String webhook = null; Connect con = new
		 * Connect(
		 * "https://hooks.slack.com/services/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"
		 * );
		 * 
		 * json = new JSONObject(); json.put("text",
		 * tmp.get(topicStructure.getDeviceId()).toString()); webhook =
		 * con.post(con.getURL(), json);
		 * 
		 * 
		 * 
		 * for(String item : list){ JSONObject json = null; String webhook =
		 * null; Connect con = new Connect(
		 * "https://hooks.slack.com/services/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"
		 * );
		 * 
		 * json = new JSONObject(); json.put("text", item); webhook =
		 * con.post(con.getURL(), json); }
		 * 
		 * //phaseNum = (JSONObject)phaseId.get("phase1"); //deviceId =
		 * (JSONObject)phaseNum.get("devices");
		 * 
		 * 
		 * } catch (ParseException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } } });
		 * 
		 * } catch (NumberFormatException e) { e.getMessage(); }
		 */

		collector.emit(new Values(spoutSource, topicStructureArray, serverIdCheck, brokerIdCheck, deviceIdCheck,
				phaseRoadMapIdCheck, mapIdCheck));

		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("spoutSource", "topicStructureArray", "serverIdCheck", "brokerIdCheck",
				"deviceIdCheck", "phaseRoadMapIdCheck", "mapIdCheck"));
	}
}
