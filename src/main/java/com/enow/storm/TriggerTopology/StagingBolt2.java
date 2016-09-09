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
import org.json.simple.parser.JSONParser;

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
		JSONObject _jsonObject;
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
		JSONArray deviceId;
		JSONArray ifLastMapId;
		boolean ifLastMapIdCheck = false;
		ArrayList<Integer> waitMapId = new ArrayList<Integer>();
		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();
		
		_jsonObject = (JSONObject)input.getValueByField("jsonObject");
        
		if(_jsonObject == null){
			return;
		}
		
		// Connect MongoDB
		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		// Get enow database
		MongoDatabase dbWrite = mongoClient.getDatabase("lists");

		MongoCollection<Document> serverListCollection = dbWrite.getCollection("server");
        
		
		if (serverListCollection.count(new Document("serverId", (String)_jsonObject.get("serverId"))) == 0) {
			// There isn't deviceId that matches input signal from device
			serverIdCheck = false;
		} else if (serverListCollection.count(new Document("serverId", (String)_jsonObject.get("serverId"))) == 1) {
			// There is deviceId that matches input signal from device
			serverIdCheck = true;
		} else {
			LOG.debug("There are more than two server ID on MongoDB");
			// this should not happen it's our mistake
		}
		
		// Get device collection for matching input signal from device
		MongoCollection<Document> brokerListCollection = dbWrite.getCollection("broker");

		if (brokerListCollection.count(new Document("brokerId", (String)_jsonObject.get("brokerId"))) == 0) {
			// There isn't deviceId that matches input signal from device
			brokerIdCheck = false;
		} else if (brokerListCollection.count(new Document("brokerId", (String)_jsonObject.get("brokerId"))) == 1) {
			// There is deviceId that matches input signal from device
			brokerIdCheck = true;
	        
		} else {
			// machineIdCheck = "device id : now we have a problem";
			LOG.debug("There are more than two broker ID on MongoDB");
		}
		
		// Get device collection for matching input signal from device
		MongoCollection<Document> deviceListCollection = dbWrite.getCollection("device");

		if (deviceListCollection.count(new Document("deviceId", (String)_jsonObject.get("deviceId"))) == 0) {
			// There isn't deviceId that matches input signal from device
			deviceIdCheck = false;
		} else if (deviceListCollection.count(new Document("deviceId", (String)_jsonObject.get("deviceId"))) == 1) {
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
					new Document("phaseRoadMapId", (long)_jsonObject.get("phaseRoadMapId"))) == 0) {
				// There isn't phaseRoadMapId that matches input signal from
				// device
				phaseRoadMapIdCheck = false;
			} else if (phaseRoadMapCollection.count(
					new Document("phaseRoadMapId", (long)_jsonObject.get("phaseRoadMapId"))) == 1) {
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

	
		if (!(boolean)_jsonObject.get("ack")) {
			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck) {
				try {
					iterable = phaseRoadMapCollection.find(
							new Document("phaseRoadMapId", (long)_jsonObject.get("phaseRoadMapId")));

					try {
						phaseRoadMapId = (JSONObject) jsonParser.parse(iterable.first().toJson());
						phases = (JSONObject) phaseRoadMapId.get("phases");
						phaseId = (JSONObject) phases.get("phase1");
						devices = (JSONObject) phaseId.get("devices");
						deviceId = (JSONArray) devices.get((String)_jsonObject.get("deviceId"));

						_jsonObject.put("phaseId", "phase1");

						for (int n = 0; n < deviceId.size(); n++) {
							JSONObject deviceIdList = (JSONObject) deviceId.get(n);
							String tmp = _jsonObject.toJSONString();
							JSONParser parser = new JSONParser();
							try {
								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 1) {
									mapIdCheck = true;
									
									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(tmp);
									tmpJsonObject.put("mapId",1);
									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 3) {
									mapIdCheck = true;
									
									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(tmp);
									tmpJsonObject.put("mapId",3);
									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 5) {
									mapIdCheck = true;
									
									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(tmp);
									tmpJsonObject.put("mapId",5);
									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 7) {
									mapIdCheck = true;
									
									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(tmp);
									tmpJsonObject.put("mapId",7);
									_jsonArray.add(tmpJsonObject);
								} else {
								}

								if (Double.parseDouble(deviceIdList.get("mapId").toString()) == 9) {
									mapIdCheck = true;
									
									JSONObject tmpJsonObject = new JSONObject();
									tmpJsonObject = (JSONObject) parser.parse(tmp);
									tmpJsonObject.put("mapId",9);
									_jsonArray.add(tmpJsonObject);
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
		}else if((boolean)_jsonObject.get("ack")){
		
		}else{
			
		}

		collector.emit(new Values(_jsonArray, serverIdCheck, brokerIdCheck, deviceIdCheck,
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
		declarer.declare(new Fields("jsonArray", "serverIdCheck", "brokerIdCheck",
				"deviceIdCheck", "phaseRoadMapIdCheck", "mapIdCheck"));
	}
}
