package com.enow.storm.ActionTopology;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Properties;

public class CallingFeedBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(CallingFeedBolt.class);
    protected static final String _KAFKA_FEED = "feed";
    protected static final String _KAFKA_PROCEED = "proceed";
    private OutputCollector _collector;
    private Producer<String, String> _producer;
    private Properties _props;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _props = new Properties();
        _props.put("producer.type", "sync");
        _props.put("batch.size", "1");
        _props.put("bootstrap.servers", "localhost:9092");
        _props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        _props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        _producer = new KafkaProducer<>(_props);
    }

    @Override
    public void execute(Tuple input) {
        JSONObject _jsonObject;
        _jsonObject = (JSONObject) input.getValueByField("jsonObject");

        Boolean verified = (Boolean) _jsonObject.get("verified");
        if(verified) {
            Boolean lastNode = (Boolean) _jsonObject.get("lastNode");
            String temp;
            JSONArray outingJSON = (JSONArray) _jsonObject.get("outingNode");
            String[] outingNodes = null;
            if (outingJSON != null) {
                outingNodes = new String[outingJSON.size()];
                for (int i = 0; i < outingJSON.size(); i++)
                    outingNodes[i] = (String) outingJSON.get(i);
            }
            if (outingNodes != null) {
                // OutingNodes exist
                ProducerRecord<String, String> nodeData = new ProducerRecord<>(_KAFKA_FEED, _jsonObject.toJSONString());
                _producer.send(nodeData);
                for (String outingNode : outingNodes) {
                    // change mapId to outingNode
                    JSONObject tempJSON = _jsonObject;
                    tempJSON.put("mapId", outingNode);
                    temp = tempJSON.toJSONString();
                    if (!lastNode) {
                        nodeData = new ProducerRecord<>(_KAFKA_PROCEED, temp);
                        _producer.send(nodeData);
                    }
                }
            } else {
                // OutingNodes don't exist
                // Maybe This node is the last node of sequence or alone
                temp = _jsonObject.toJSONString();
                ProducerRecord<String, String> nodeData = new ProducerRecord<>(_KAFKA_FEED, temp);
                _producer.send(nodeData);
                if (!lastNode) {
                    nodeData = new ProducerRecord<>(_KAFKA_PROCEED, temp);
                    _producer.send(nodeData);
                } else {

                }
            }
        }
        _collector.emit(new Values(_jsonObject));
        try {
            _LOG.debug("input = [" + input + "]");
            _collector.ack(input);
        } catch (Exception e) {
            _LOG.warn("input = [" + input + "]");
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}