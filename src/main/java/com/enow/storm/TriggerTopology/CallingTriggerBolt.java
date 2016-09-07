package com.enow.storm.TriggerTopology;

import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.enow.dto.TopicStructure;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class CallingTriggerBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(CallingTriggerBolt.class);
	private OutputCollector collector;
	private Properties props;
	private Producer<String, String> producer;
	private TopicStructure topicStructure;
	private String spoutSource;
	boolean serverIdCheck;
	boolean brokerIdCheck;
	boolean deviceIdCheck;
	boolean phaseRoadMapIdCheck;
	boolean mapIdCheck;
	ArrayList<TopicStructure> topicStructureArray;

	@Override

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
		topicStructure = new TopicStructure();
	}

	@Override
	public void execute(Tuple input) {
		spoutSource = input.getStringByField("spoutSource");
		topicStructureArray = (ArrayList<TopicStructure>) input.getValueByField("topicStructureArray");
		deviceIdCheck = input.getBooleanByField("deviceIdCheck");
		phaseRoadMapIdCheck = input.getBooleanByField("phaseRoadMapIdCheck");
		mapIdCheck = input.getBooleanByField("mapIdCheck");
		serverIdCheck = input.getBooleanByField("serverIdCheck");
		brokerIdCheck = input.getBooleanByField("brokerIdCheck");
		if (spoutSource.equals("trigger")) {
			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck) {
				/*
				 * json = new JSONObject(); json.put("spoutName","trigger");
				 * json.put("corporationName", ts.getCorporationName());
				 * json.put("serverId",ts.getServerId());
				 * json.put("brokerId",ts.getBrokerId());
				 * json.put("deviceId",ts.getDeviceId());
				 * json.put("phaseRoadMapId",ts.getPhaseRoadMapId());
				 * json.put("metadata",msg); ProducerRecord<String, String> data
				 * = new ProducerRecord<String, String>("trigger",
				 * json.toString()); producer.send(data);
				 */

				for (TopicStructure tmp : topicStructureArray) {
					ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
							"trigger," + tmp.output());
					producer.send(data);
				}
			} else {
				/*
				 * json = new JSONObject(); json.put("error","error");
				 */
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
						"error : "+ "serverIdCheck = " + serverIdCheck + "brokerIdCheck = " + brokerIdCheck + "machinIdCheck = " + deviceIdCheck + " phaseRoadMapIdCheck = "
								+ phaseRoadMapIdCheck + " mapIdCheck = " + mapIdCheck);
				producer.send(data);
			}
		}else if(spoutSource.equals("proceed")){
			if (serverIdCheck && brokerIdCheck && deviceIdCheck && phaseRoadMapIdCheck && mapIdCheck) {
				/*
				 * json = new JSONObject(); json.put("spoutName","trigger");
				 * json.put("corporationName", ts.getCorporationName());
				 * json.put("serverId",ts.getServerId());
				 * json.put("brokerId",ts.getBrokerId());
				 * json.put("deviceId",ts.getDeviceId());
				 * json.put("phaseRoadMapId",ts.getPhaseRoadMapId());
				 * json.put("metadata",msg); ProducerRecord<String, String> data
				 * = new ProducerRecord<String, String>("trigger",
				 * json.toString()); producer.send(data);
				 */

				for (TopicStructure tmp : topicStructureArray) {
					ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
							"proceed," + tmp.output());
					producer.send(data);
				}
			} else {
				/*
				 * json = new JSONObject(); json.put("error","error");
				 */
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger",
						"error : "+ "serverIdCheck = " + serverIdCheck + "brokerIdCheck = " + brokerIdCheck + "machinIdCheck = " + deviceIdCheck + " phaseRoadMapIdCheck = "
								+ phaseRoadMapIdCheck + " mapIdCheck = " + mapIdCheck);
				producer.send(data);
			}
		}else{
			//spoutSource가 Trigger나 proceed가 아닌 경
		}
		
		collector.emit(new Values(topicStructure.output()));

		try {
			_LOG.debug("input = [" + input + "]");
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