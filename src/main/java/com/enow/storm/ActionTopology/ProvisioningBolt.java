package com.enow.storm.ActionTopology;

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.NodeDTO;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class ProvisioningBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    private IRedisDB _redis;
    private OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        String redisIp = (String) conf.get("redis.ip");
        Long lredisPort = (Long) conf.get("redis.port");
        int  redisPort = lredisPort.intValue();
        _redis = RedisDB.getInstance(redisIp, redisPort);
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        JSONObject _jsonObject = new JSONObject();
        org.apache.storm.shade.org.json.simple.JSONObject _jsonObject_conversion = (org.apache.storm.shade.org.json.simple.JSONObject) input.getValue(0);
        String _jsonObject_str = _jsonObject_conversion.toJSONString();
        JSONParser _parser = new JSONParser();

        try {
            _jsonObject = (JSONObject) _parser.parse(_jsonObject_str);
        } catch (ParseException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        Boolean verified = (Boolean) _jsonObject.get("verified");
        Boolean lastNode = (Boolean) _jsonObject.get("lastNode");
        // Confirm verification
        if (verified) {
            // Store this node for subsequent node
            String result = "nothing";
            try {
                NodeDTO dto = _redis.jsonObjectToNode(_jsonObject);
                result = _redis.addNode(dto);
                _LOG.debug("Succeed in inserting current node to Redis : " + result);
            } catch (Exception e) {
                e.printStackTrace();
                _LOG.debug("Fail in inserting current node to Redis : " + result);
            }
            // Check this node is the last node
            if(lastNode) {
                // When this node is the last node, delete the node that will not be used
                _redis.deleteLastNode(_redis.jsonObjectToNode(_jsonObject));
            }
        }
        // Go to next bolt
        _collector.emit(new Values(_jsonObject));
        try {
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