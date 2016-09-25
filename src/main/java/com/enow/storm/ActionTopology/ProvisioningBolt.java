package com.enow.storm.ActionTopology;

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.facility.DAOFacility;
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

import java.util.Map;

public class ProvisioningBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    private IRedisDB _redis;
    private OutputCollector _collector;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _redis = RedisDB.getInstance();
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        JSONObject _jsonObject;

        _jsonObject = (JSONObject) input.getValueByField("jsonObject");
        Boolean verified = (Boolean) _jsonObject.get("verified");
        Boolean lastNode = (Boolean) _jsonObject.get("lastNode");
        // Confirm verification
        if (verified) {
            // Check this node is the last node
            if(!lastNode) {
                // When this node is the last node, delete the node that will not be used
                _redis.updateNode(_redis.jsonObjectToNode(_jsonObject));
            } else {
                // When this node isn't the last node, the reference value is decreased
                _redis.updateRefer(_redis.jsonObjectToNode(_jsonObject));
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