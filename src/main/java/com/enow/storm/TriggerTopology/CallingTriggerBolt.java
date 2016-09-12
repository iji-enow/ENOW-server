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

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class CallingTriggerBolt extends BaseRichBolt {
	protected static final Logger LOG = LogManager.getLogger(CallingTriggerBolt.class);
	private OutputCollector collector;
	private Properties props;

	@Override

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	@Override
	public void execute(Tuple input) {
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		String spoutSource;
		boolean serverIdCheck;
		boolean brokerIdCheck;
		boolean deviceIdCheck;
		boolean phaseRoadMapIdCheck;
		boolean mapIdCheck;
		ArrayList<JSONObject> _jsonArray = new ArrayList<JSONObject>();

		_jsonArray = (ArrayList<JSONObject>) input.getValueByField("jsonArray");
		deviceIdCheck = input.getBooleanByField("deviceIdCheck");
		phaseRoadMapIdCheck = input.getBooleanByField("phaseRoadMapIdCheck");
		mapIdCheck = input.getBooleanByField("mapIdCheck");
		serverIdCheck = input.getBooleanByField("serverIdCheck");
		brokerIdCheck = input.getBooleanByField("brokerIdCheck");

		if (_jsonArray == null) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
					"error : " + "serverIdCheck = " + serverIdCheck + " brokerIdCheck = " + brokerIdCheck
							+ " machinIdCheck = " + deviceIdCheck + " phaseRoadMapIdCheck = " + phaseRoadMapIdCheck
							+ " mapIdCheck = " + mapIdCheck);
			producer.send(data);

			collector.emit(new Values("error"));
		} else {
			for (JSONObject tmpJsonObject : _jsonArray) {
				if (!(boolean) tmpJsonObject.get("ack")) {
					if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck) {
						ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
								tmpJsonObject.toJSONString());
						producer.send(data);
						collector.emit(new Values(tmpJsonObject.toJSONString()));
					} else {
						ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
								"error : " + "serverIdCheck = " + serverIdCheck + " brokerIdCheck = " + brokerIdCheck
										+ " machinIdCheck = " + deviceIdCheck + " phaseRoadMapIdCheck = "
										+ phaseRoadMapIdCheck + " mapIdCheck = " + mapIdCheck);
						producer.send(data);

						collector.emit(new Values("error"));
					}
				} else {

				}
			}
		}
		/*
		 * if (spoutSource.equals("trigger")) { if (serverIdCheck &&
		 * brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck)
		 * { for (TopicStructure tmp : topicStructureArray) { if
		 * (tmp.isLastMapId()) { String a = ""; for (int i = 0; i <
		 * tmp.getWaitMapId().size(); i++) { a += tmp.getWaitMapId().get(i) +
		 * " "; } ProducerRecord<String, String> data = new
		 * ProducerRecord<String, String>("trigger", "trigger," + tmp.output() +
		 * " " + a); producer.send(data); collector.emit(new
		 * Values(tmp.output())); } else { ProducerRecord<String, String> data =
		 * new ProducerRecord<String, String>("trigger", "trigger," +
		 * tmp.output()); producer.send(data); collector.emit(new
		 * Values(tmp.output())); } } } else { ProducerRecord<String, String>
		 * data = new ProducerRecord<String, String>("trigger", "error : " +
		 * "serverIdCheck = " + serverIdCheck + " brokerIdCheck = " +
		 * brokerIdCheck + " machinIdCheck = " + deviceIdCheck +
		 * " phaseRoadMapIdCheck = " + phaseRoadMapIdCheck + " mapIdCheck = " +
		 * mapIdCheck); producer.send(data); collector.emit(new
		 * Values("error : " + "serverIdCheck = " + serverIdCheck +
		 * " brokerIdCheck = " + brokerIdCheck + " machinIdCheck = " +
		 * deviceIdCheck + " phaseRoadMapIdCheck = " + phaseRoadMapIdCheck +
		 * " mapIdCheck = " + mapIdCheck)); } } else if
		 * (spoutSource.equals("proceed")) { if (serverIdCheck && brokerIdCheck
		 * && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck) { for
		 * (TopicStructure tmp : topicStructureArray) { ProducerRecord<String,
		 * String> data = new ProducerRecord<String, String>("trigger",
		 * "proceed," + tmp.output()); producer.send(data); collector.emit(new
		 * Values(tmp.output())); } } else {
		 * 
		 * json = new JSONObject(); json.put("error","error");
		 * 
		 * ProducerRecord<String, String> data = new ProducerRecord<String,
		 * String>("trigger", "error : " + "serverIdCheck = " + serverIdCheck +
		 * " brokerIdCheck = " + brokerIdCheck + " machinIdCheck = " +
		 * deviceIdCheck + " phaseRoadMapIdCheck = " + phaseRoadMapIdCheck +
		 * " mapIdCheck = " + mapIdCheck); producer.send(data);
		 * collector.emit(new Values("error : " + "serverIdCheck = " +
		 * serverIdCheck + " brokerIdCheck = " + brokerIdCheck +
		 * " machinIdCheck = " + deviceIdCheck + " phaseRoadMapIdCheck = " +
		 * phaseRoadMapIdCheck + " mapIdCheck = " + mapIdCheck)); } } else { //
		 * spoutSource가 Trigger나 proceed가 아닌 경 }
		 */
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("triggerTopologyResult"));
	}
}