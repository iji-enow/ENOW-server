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
    ConcurrentHashMap<String, JSONObject> _executedNode = new ConcurrentHashMap<>();
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
        String temp;
        String mapId = (String) _jsonObject.get("mapId");
        if (proceed) {
            JSONArray waitingJSON = (JSONArray) _jsonObject.get("waitingPeer");
            String[] waitingPeers = new String[waitingJSON.size()];
            if (waitingPeers != null) {
                for (int i = 0; i < waitingJSON.size(); i++)
                    waitingPeers[i] = (String) waitingJSON.get(i);
                for (String waitingPeer : waitingPeers) {
                    temp = _executedNode.get(waitingPeer).toJSONString();
                    ProducerRecord<String, String> peerData = new ProducerRecord<>("feed", temp);
                    producer.send(peerData);
                }
            }
            ProducerRecord<String, String> data = new ProducerRecord<>("feed", _jsonObject.toJSONString());
            producer.send(data);
        } else {
            _executedNode.put(mapId, _jsonObject);
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