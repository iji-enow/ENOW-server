package com.enow.storm.ActionTopology;

import com.enow.persistence.dto.NodeDTO;
import com.enow.persistence.dto.StatusDTO;
import com.enow.persistence.redis.IRedisDB;
import com.enow.persistence.redis.RedisDB;
import com.esotericsoftware.minlog.Log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchedulingBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(SchedulingBolt.class);
	private IRedisDB _redis;
	private OutputCollector _collector;
	private JSONParser _parser;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		String redisIp = (String) conf.get("redis.ip");
		Long lredisPort = (Long) conf.get("redis.port");
		int redisPort = lredisPort.intValue();

		_redis = RedisDB.getInstance(redisIp, redisPort);
		_collector = collector;
		_parser = new JSONParser();
	}

	@Override
	public void execute(Tuple input) {

		_LOG.debug("Entering SchedulingBolt");

		JSONObject _jsonObject;
		if ((null == input.toString()) || (input.toString().length() == 0)) {
			_LOG.warn("error:1");
			return;
		}
		// Parsing JSONString to JSONObject
		String jsonString = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
		try {
			_jsonObject = (JSONObject) _parser.parse(jsonString);
			_LOG.debug("Succeed in inserting messages to _jsonObject : " + _jsonObject.toJSONString());
		} catch (ParseException e1) {
			e1.printStackTrace();
			// _LOG.fatal("Fail in inserting messages to _jsonObject");
			_LOG.warn("error:2");
			return;
		}

		Boolean order = (Boolean) _jsonObject.get("order");
		Boolean lambda = (Boolean) _jsonObject.get("lambda");
		String topic = (String) _jsonObject.get("topic");
		// order and lambda nodes aren't related to devices
		// So skip this part
		if (!(order || lambda)) {
			// Ready to get the status of device we need
			StatusDTO statusDTO = _redis.getStatus(topic);
			String temp = statusDTO.getPayload();
			try {
				_jsonObject.put("payload", _parser.parse(temp));
				_LOG.debug("Succeed in inserting status to _jsonObject : " + _jsonObject.toJSONString());
			} catch (ParseException e1) {
				e1.printStackTrace();
				_LOG.debug("Fail in inserting status to _jsonObject");
				_LOG.warn("error:3");
				return;
			}
		}

		String roadMapId = (String) _jsonObject.get("roadMapId");
		String nodeId = (String) _jsonObject.get("nodeId");
		System.out.println(_jsonObject.toJSONString());
		boolean initNode = (boolean) _jsonObject.get("initNode");
		JSONArray incomingJSON = (JSONArray) _jsonObject.get("incomingNode");
		String[] incomingNodes = null;
		if (incomingJSON != null) {
			if (initNode) {
				_LOG.debug("This is initNode : " + _jsonObject.toJSONString());
			} else {
				incomingNodes = new String[incomingJSON.size()];
				// If this node have incoming nodes...
				for (int i = 0; i < incomingJSON.size(); i++)
					incomingNodes[i] = (String) incomingJSON.get(i);
				// Put the previous data incoming nodes have in _jsonObject
				if (incomingNodes != null) {
					JSONObject tempJSON = new JSONObject();
					List<NodeDTO> checker = new ArrayList<>();
					String id;
					for (String incomingNodeId : incomingNodes) {
						id = _redis.toID(roadMapId, incomingNodeId);
						NodeDTO tempDTO = _redis.getNode(id);
						if (tempDTO != null) {
							checker.add(tempDTO);
						}
					}
					// If incomingJSON is empty, the verified value is going to
					// be false
					if (checker.size() == incomingJSON.size()) {
						NodeDTO redundancy = _redis.getNode(_redis.toID(roadMapId, nodeId));
						// If Current nodeId has been saved on Redis
						if (redundancy == null) {
							JSONArray arr_temp = new JSONArray();
							for (NodeDTO node : checker) {
								_redis.updateRefer(node);
								try {
									arr_temp.add(_parser.parse(node.getPayload()));
								} catch (ParseException e1) {
									e1.printStackTrace();
									_LOG.debug("Fail in inserting status to _jsonObject");
									_LOG.warn("error:4");
									return;
								}
							}
							_jsonObject.put("previousData", arr_temp);
							_LOG.debug("Succeed in inserting previousData to _jsonObject : " + tempJSON.toJSONString());
						} else {
							_jsonObject.put("verified", false);
							_LOG.debug("This _jsonObject isn't verified : " + tempJSON.toJSONString());
						}
					} else {
						_jsonObject.put("verified", false);
						_LOG.debug("This _jsonObject isn't verified : " + tempJSON.toJSONString());
					}
				}
			}
		}

		// Go to next bolt
		_collector.emit(new Values(_jsonObject));
		try {
			_LOG.info(_jsonObject.get("roadMapId") + "," + _jsonObject.get("nodeId") + "|" + _jsonObject.get("topic")
					+ "|" + _jsonObject.toString());
			_collector.ack(input);
		} catch (Exception e) {
			_LOG.warn("ack failed");
			_collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonObject"));
	}
}