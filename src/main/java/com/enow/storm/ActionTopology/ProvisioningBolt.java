package com.enow.storm.ActionTopology;

import com.enow.dto.TopicStructure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class ProvisioningBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    private OutputCollector _collector;
    private JSONParser _parser;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _parser = new JSONParser();
    }

    @Override
    public void execute(Tuple input) {

        JSONObject _jsonObject;

        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }

        _jsonObject = (JSONObject) input.getValueByField("jsonObject");
        // message 받는거 처리
        String message = (String) input.getValueByField("message");

       if ((Boolean) _jsonObject.get("proceed")) {
            _jsonObject.put("ack", true);
        }

        String[] outingPeers = (String[]) _jsonObject.get("outingPeer");
        if (outingPeers[0] != "0"){
            message
        }

        System.out.println(_jsonObject.toJSONString());

        _collector.emit(new Values(_jsonObject));
        try {
            _LOG.debug("input = [" + input + "]");
            _collector.ack(input);
        } catch (Exception e) {
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("jsonObject"));
    }
}