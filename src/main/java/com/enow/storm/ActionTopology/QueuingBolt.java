package com.enow.storm.ActionTopology;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enow.dto.TopicStructure;

public class QueuingBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private ConcurrentHashMap<UUID, Values> pending;
    private int index = 0;
    private OutputCollector collector;
    // Structure that stores MQTT topic seperately
    private TopicStructure topicStructure;
    // Kafka topic name
    private String spoutName;
    // MQTT topics
    private String topic;
    // Data
    private String msg;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.pending = new ConcurrentHashMap<>();
        this.collector = collector;
        topicStructure = new TopicStructure();
        spoutName = "";
        topic = "";
        msg = "";
    }

    @Override
    public void execute(Tuple input) {
        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }
        UUID msgId = UUID.randomUUID();
        // tuple to String
        String inputStr = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
        // Insert data
        String msg = inputStr.split(" ")[1];
        // Insert Kafka topic name
        String spoutName = inputStr.split(" ")[0];
        // Insert MQTT topic name
        String topic = inputStr.split("/")[1];

        // build topicStructure
        topicStructure.setCorporationName(topic.split("/")[0]);
        topicStructure.setServerId(topic.split("/")[1]);
        topicStructure.setBrokerId(topic.split("/")[2]);
        topicStructure.setDeviceId(topic.split("/")[3]);
        topicStructure.setPhaseRoadMapId(topic.split("/")[4]);
        // Insert data and generate Hashmap Id for queue
        Values values = new Values(msg);
        pending.put(msgId, values);
        index++;
        if (index >= inputStr.length()) {
            index = 0;
        }
        if (spoutName == "trigger") {
            // trigger enow/serverId/brokerId/deviceId/phaseRoadMapId msg

            if ((null == msg) || (msg.length() == 0)) {
                return;
            }
            collector.emit(values);
            try {
                LOG.debug("input = [" + input + "]");
                collector.ack(input);
                pending.remove(msgId);
            } catch (Exception e) {
                collector.fail(input);
                collector.emit(pending.get(msgId));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }
}