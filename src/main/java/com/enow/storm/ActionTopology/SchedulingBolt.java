package com.enow.storm.ActionTopology;


import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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

public class SchedulingBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private OutputCollector collector;
    private TopicStructure topicStructure;
    private String spoutName;
    private String topic;
    private String msg;
    private Queue buffer;
    private JSONObject json;

    @Override

    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        topicStructure = new TopicStructure();
        spoutName = "";
        topic = "";
        msg = "";
        buffer = new PriorityQueue();
    }

    @Override
    public void execute(Tuple input) {
        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }

        String inputStr = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
        String msg = inputStr.split(" ")[1];

        String spoutName = inputStr.split(" ")[0];
        String topic = inputStr.split("/")[1];

        topicStructure.setCorporationName(topic.split("/")[0]);
        topicStructure.setServerId(topic.split("/")[1]);
        topicStructure.setBrokerId(topic.split("/")[2]);
        topicStructure.setDeviceId(topic.split("/")[3]);
        topicStructure.setPhaseRoadMapId(topic.split("/")[4]);

        if (spoutName == "trigger") {
            // trigger enow/serverId/brokerId/deviceId/phaseRoadMapId msg

            if ((null == msg) || (msg.length() == 0)) {
                return;
            }
            collector.emit(new Values(msg));
            try {
                LOG.debug("input = [" + input + "]");
                collector.ack(input);
            } catch (Exception e) {
                collector.fail(input);
            }
        }
        if (spoutName == "status") {
            try {
                // status enow/serverId/brokerId/deviceId/phaseRoadMapId JSON
                JSONParser jsonParser = new JSONParser();
                JSONObject json = (JSONObject) jsonParser.parse(msg);
                String deviceStatus = json.get("status").toString();
                String metadata = json.get("metadata").toString();

            }catch(ParseException e){

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }
}