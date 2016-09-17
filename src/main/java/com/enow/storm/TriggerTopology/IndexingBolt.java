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
		MongoDAO mongoDao;

		String msg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

		try {
			mongoDao = new MongoDAO("127.0.0.1",27017);
		} catch (UnknownHostException e1) {
			LOG.debug("error : 1");
			return;
		}
		
		if (input.toString().length() == 0) {
			LOG.debug("error : 2");
			return;
		}

		if (input.getSourceComponent().equals("event-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("roadMapId")) {
					_jsonObject.put("spoutName", "event");
				} else {
					LOG.debug("error : 3");
					return;
				}
			} catch (ParseException e) {
				LOG.debug("error : 4");
				return;
			}
		} else if (input.getSourceComponent().equals("order-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("corporationName") && _jsonObject.containsKey("serverId")
						&& _jsonObject.containsKey("brokerId") && _jsonObject.containsKey("roadMapId")
						&& _jsonObject.containsKey("deviceId") && _jsonObject.containsKey("payload")) {
				} else {
					LOG.debug("error : 5");
					return;
				}

				mongoDao.setDBCollection("lists", "server");
				
				if (mongoDao.collectionCount(new Document("serverId", (String) _jsonObject.get("serverId"))) == 0) {
					serverIdCheck = false;
				} else if (mongoDao.collectionCount(new Document("serverId", (String) _jsonObject.get("serverId"))) == 1) {
					serverIdCheck = true;
				} else {
					serverIdCheck = false;
					LOG.debug("There are more than two server ID on MongoDB");
				}

				// Get device collection for matching input signal from device
				mongoDao.setCollection("broker");

				if (mongoDao.collectionCount(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 0) {
					brokerIdCheck = false;
				} else if (mongoDao.collectionCount(new Document("brokerId", (String) _jsonObject.get("brokerId"))) == 1) {
					brokerIdCheck = true;

				} else {
					brokerIdCheck = false;
					LOG.debug("There are more than two broker ID on MongoDB");
				}

				// Get device collection for matching input signal from device

				mongoDao.setCollection("device");

				if (mongoDao.collectionCount(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 0) {
					deviceIdCheck = false;
				} else if (mongoDao.collectionCount(new Document("deviceId", (String) _jsonObject.get("deviceId"))) == 1) {
					deviceIdCheck = true;
				} else {
					deviceIdCheck = false;
					LOG.debug("There are more than two machine ID on MongoDB");
				}

				mongoDao.setDBCollection("enow", "roadMap");
				

				if (mongoDao.collectionCount(new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 0) {
					roadMapIdCheck = false;
				} else if (mongoDao.collectionCount(new Document("roadMapId", (String) _jsonObject.get("roadMapId"))) == 1) {
					roadMapIdCheck = true;
				} else {
					roadMapIdCheck = false;
					LOG.debug("There are more than two Phase Road-map Id on MongoDB");
				}

				if (serverIdCheck && brokerIdCheck && deviceIdCheck && roadMapIdCheck) {
					_jsonObject.put("spoutName", "order");
				} else {
					LOG.debug("error : 6");
					return;
				}
			} catch (ParseException e) {
				LOG.debug("error : 7");
				return;
			}

		} else if (input.getSourceComponent().equals("proceed-spout")) {
			try {
				_jsonObject = (JSONObject) parser.parse(msg);

				if (_jsonObject.containsKey("order") && _jsonObject.containsKey("roadMapId")
						&& _jsonObject.containsKey("mapId") && _jsonObject.containsKey("payload")
						&& _jsonObject.containsKey("incomingNode") && _jsonObject.containsKey("outingNode")
						&& _jsonObject.containsKey("previousData") && _jsonObject.containsKey("topic")
						&& _jsonObject.containsKey("lastNode")&& _jsonObject.containsKey("verified")) {

					_jsonObject.put("spoutName", "proceed");
				} else {
					LOG.debug("error : 8");
					return;
				}
			} catch (ParseException e) {
				LOG.debug("error : 9");
				return;
			}
		} else {
			LOG.debug("error : 10");
			return;
		}

		collector.emit(new Values(_jsonObject));

		try {
			LOG.debug(_jsonObject.toJSONString());
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