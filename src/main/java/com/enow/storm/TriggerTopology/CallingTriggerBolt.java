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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enow.dto.TopicStructure;

import com.enow.storm.Connect;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.Properties;

public class CallingTriggerBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(CallingTriggerBolt.class);
    private OutputCollector collector;
    private Properties props;
    private Producer<String, String> producer;
    private TopicStructure topicStructure;
    private JSONObject json;

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
        if (null == input.getValueByField("topicStructure")) {
            return;
        }

        topicStructure = (TopicStructure) input.getValueByField("topicStructure");
        final boolean machineIdCheck = input.getBooleanByField("machineIdCheck");
        final boolean phaseRoadMapIdCheck = input.getBooleanByField("phaseRoadMapIdCheck");
        final boolean mapIdCheck = input.getBooleanByField("mapIdCheck");

        if (machineIdCheck && phaseRoadMapIdCheck && mapIdCheck) {
        	/*
        	json = new JSONObject();
            json.put("spoutName","trigger");
            json.put("corporationName", ts.getCorporationName());
            json.put("serverId",ts.getServerId());
            json.put("brokerId",ts.getBrokerId());   
            json.put("deviceId",ts.getDeviceId());
            json.put("phaseRoadMapId",ts.getPhaseRoadMapId());
            json.put("metadata",msg);
            ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger", json.toString());
            producer.send(data);
            */
        	ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger", "trigger " + topicStructure.output());
            producer.send(data);
        }else{
        	/*
        	json = new JSONObject();
            json.put("error","error");
            */
        	ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger", "error : " + "machinIdCheck = " +machineIdCheck + " phaseRoadMapIdCheck = " + phaseRoadMapIdCheck + " mapIdCheck = " + mapIdCheck );
            producer.send(data);
        }

        try {
            LOG.debug("input = [" + input + "]");
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}