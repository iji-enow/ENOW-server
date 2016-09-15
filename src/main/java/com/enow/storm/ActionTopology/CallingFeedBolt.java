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
import java.util.concurrent.ConcurrentHashMap;

public class CallingFeedBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(CallingFeedBolt.class);
    private OutputCollector _collector;
    private Producer<String, String> producer;
    private Properties props;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        props = new Properties();
        props.put("producer.type", "sync");
        props.put("batch.size", "1");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple input) {
        JSONObject _jsonObject;

        _jsonObject = (JSONObject) input.getValueByField("jsonObject");
        Boolean proceed = (Boolean) _jsonObject.get("proceed");
        Integer order = (Integer) _jsonObject.get("order");
        String temp;

        if (proceed) {
            JSONArray outingJSON = (JSONArray) _jsonObject.get("outingPeer");
            String[] outingPeers = new String[outingJSON.size()];
            if (outingJSON != null) {
                for (int i = 0; i < outingJSON.size(); i++)
                    outingPeers[i] = (String) outingJSON.get(i);
            }
            if (outingPeers != null) {
                // OutingNodes exist
                for (String outingPeer : outingPeers) {
                    // 맵 아이디 변환작업
                    _jsonObject.put("mapId", outingPeer);
                    temp = _jsonObject.toJSONString();
                    ProducerRecord<String, String> nodeData = new ProducerRecord<>("feed", temp);
                    producer.send(nodeData);
                }
            } else {
                // OutingNodes don't exist
                // Maybe This node is the last node of sequence or alone
                temp = _jsonObject.toJSONString();
                ProducerRecord<String, String> nodeData = new ProducerRecord<>("feed", temp);
                producer.send(nodeData);
                if(order == 1 || order == 0){
                    nodeData = new ProducerRecord<>("proceed", temp);
                    producer.send(nodeData);
                }
            }
        }
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
    }
}