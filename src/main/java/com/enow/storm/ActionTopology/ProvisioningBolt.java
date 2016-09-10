package com.enow.storm.ActionTopology;

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

        _jsonObject = (JSONObject) input.getValueByField("jsonObject");
        JSONObject messageJSON = (JSONObject) input.getValueByField("message");
        String message = (String) messageJSON.get("message");
        Boolean proceed = (Boolean) _jsonObject.get("proceed");
        if (proceed) {
            _jsonObject.put("ack", true);
        } else {
            _jsonObject.put("ack", false);
        }

        JSONArray outingJSON = (JSONArray) _jsonObject.get("incomingPeer");
        String[] outingPeers = new String[outingJSON.size()];
        for(int i = 0; i<outingJSON.size(); i++ )
            outingPeers[i] = (String)outingJSON.get(i);
        if (outingPeers[0] != "0") {
            _jsonObject.put("message", message);
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