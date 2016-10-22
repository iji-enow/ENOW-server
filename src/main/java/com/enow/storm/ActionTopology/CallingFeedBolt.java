package com.enow.storm.ActionTopology;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.esotericsoftware.minlog.Log;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class CallingFeedBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(CallingFeedBolt.class);
	protected static final String _KAFKA_FEED = "feed";
	protected static final String _KAFKA_PROCEED = "proceed";
	private OutputCollector _collector;
	private Producer<String, String> _producer;
	private Properties _props;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		String kafkaProperties = (String) conf.get("kafka.properties");
		_collector = collector;
		_props = new Properties();
		_props.put("producer.type", "sync");
		_props.put("batch.size", "1");
		_props.put("bootstrap.servers", kafkaProperties);
		_props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		_props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		_producer = new KafkaProducer<>(_props);
	}

	@Override
	public void execute(Tuple input) {
		JSONObject _jsonObject;
		JSONParser parser = new JSONParser();
		_jsonObject = (JSONObject) input.getValueByField("jsonObject");
		String _jsonString = _jsonObject.toJSONString();
		ArrayList<JSONObject> _jsonArrayFeed = new ArrayList<JSONObject>();
		ArrayList<JSONObject> _jsonArrayProceed = new ArrayList<JSONObject>();

		Boolean verified = (Boolean) _jsonObject.get("verified");
		// Confirm verification
		if (verified) {
			Boolean lastNode = (Boolean) _jsonObject.get("lastNode");
			Boolean lambda = (Boolean) _jsonObject.get("lambda");
			String tempString;
			JSONArray outingJSON = (JSONArray) _jsonObject.get("outingNode");
			String[] outingNodes = null;
			if (outingJSON != null) {
				outingNodes = new String[outingJSON.size()];
				for (int i = 0; i < outingJSON.size(); i++)
					outingNodes[i] = (String) outingJSON.get(i);
			}
			// If the node has no outingNodes
			if (outingNodes != null) {
				// OutingNodes exist
				ProducerRecord<String, String> nodeData = new ProducerRecord<>(_KAFKA_FEED, _jsonObject.toJSONString());
				// If this code is for lambda
				// Won't send the data to feed topic
				if (!lambda) {
					_producer.send(nodeData);
					_jsonArrayFeed.add(_jsonObject);
				}
				for (String outingNode : outingNodes) {
					// change nodeId to outingNode
					try {
						JSONObject tempJSON = (JSONObject) parser.parse(_jsonString);

						tempJSON.put("nodeId", outingNode);
						_jsonArrayProceed.add(tempJSON);
						tempString = tempJSON.toJSONString();
						// If this node isn't the last node, send the data to
						// proceed topic
						if (!lastNode) {
							nodeData = new ProducerRecord<>(_KAFKA_PROCEED, tempString);
							_producer.send(nodeData);
						}

					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return;
					}
				}
			} else {
				// OutingNodes don't exist
				// Maybe This node is the last node of sequence or alone
				tempString = _jsonObject.toJSONString();
				ProducerRecord<String, String> nodeData = new ProducerRecord<>(_KAFKA_FEED, tempString);
				// If this code is for lambda
				// Won't send the data to feed topic
				if (!lambda) {
					_producer.send(nodeData);
					_jsonArrayFeed.add(_jsonObject);
				}
				// If this node isn't the last node, send the data to proceed
				// topic
				if (!lastNode) {
					nodeData = new ProducerRecord<>(_KAFKA_PROCEED, tempString);
					_producer.send(nodeData);
					_jsonArrayProceed.add(_jsonObject);
				} else {

				}
			}
		}
		// Go to next bolt
		_collector.emit(new Values(_jsonObject));
		// Check ack
		try {
			// Report the issues to Logger
			for (JSONObject tmp : _jsonArrayFeed) {
				_LOG.info(tmp.get("roadMapId") + ","
						+ tmp.get("nodeId") + "|" + tmp.get("topic") +"|" + tmp.toString());
			}
			for (JSONObject tmp : _jsonArrayProceed) {
				_LOG.info(tmp.get("roadMapId") + ","
						+ tmp.get("nodeId") + "|" + tmp.get("topic") +"|" + tmp.toString());
			}
			_collector.ack(input);
		} catch (Exception e) {
			Log.error("ack failed");
			_collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}