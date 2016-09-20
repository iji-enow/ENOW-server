package com.enow.storm.TriggerTopology;

import org.apache.kafka.clients.producer.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.enow.storm.Connect;
import com.esotericsoftware.minlog.Log;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class CallingTriggerBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(CallingTriggerBolt.class);
	private OutputCollector collector;
	private Properties props;

	@Override

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		//props.put("bootstrap.servers", "192.168.99.100:9092");
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	@Override
	public void execute(Tuple input) {
		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();

		_jsonArray = (ArrayList<JSONObject>) input.getValueByField("jsonArray");

		if(_jsonArray.size() == 0){
			_LOG.debug("error : 1");
			return;
		}else{
			for (JSONObject tmpJsonObject : _jsonArray) {

				ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
						tmpJsonObject.toJSONString());
				producer.send(data);
				collector.emit(new Values(tmpJsonObject.toJSONString()));
			}
		}
		
		
		try {
			_LOG.info("exited Trigger topology");
			collector.ack(input);
		} catch (Exception e) {
			Log.warn("ack failed");
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("triggerTopologyResult"));
	}
}