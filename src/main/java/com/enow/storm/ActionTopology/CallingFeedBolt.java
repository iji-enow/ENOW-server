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
    static ConcurrentHashMap<String, JSONObject> _ackedNode = new ConcurrentHashMap<>();
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
        Boolean ack = (Boolean) _jsonObject.get("ack");
        String mapId = (String) _jsonObject.get("mapId");
        String temp;
        
        if (proceed) {
            JSONArray waitingJSON = (JSONArray) _jsonObject.get("waitingPeer");
            JSONArray outingJSON = (JSONArray) _jsonObject.get("outingPeer");
            String[] waitingPeers = null;
            String[] outingPeers = null;
            _ackedNode.put(mapId, _jsonObject);
            if(waitingJSON != null){
                waitingPeers = new String[waitingJSON.size()];
                for (int i = 0; i < waitingJSON.size(); i++)
                    waitingPeers[i] = (String) waitingJSON.get(i);
            }
            if (waitingPeers != null) {
                for (String waitingPeer : waitingPeers) {
                    temp = _ackedNode.get(waitingPeer).toJSONString();
                    _ackedNode.remove(waitingPeer);
                    ProducerRecord<String, String> peerData = new ProducerRecord<>("feed", temp);
                    producer.send(peerData);
                }
            } else {
                temp = _ackedNode.get(mapId).toJSONString();
                _ackedNode.remove(mapId);
                ProducerRecord<String, String> peerData = new ProducerRecord<>("feed", temp);
                producer.send(peerData);
            }

            if(outingJSON != null){
                outingPeers = new String[outingJSON.size()];
                for (int i = 0; i < outingJSON.size(); i++)
                    outingPeers[i] = (String) outingJSON.get(i);
            }
            if (waitingPeers != null) {
                for (String waitingPeer : waitingPeers) {
                    temp = _ackedNode.get(waitingPeer).toJSONString();
                    ProducerRecord<String, String> peerData = new ProducerRecord<>("feed", temp);
                    producer.send(peerData);
                }
            }
            ProducerRecord<String, String> data = new ProducerRecord<>("feed", _jsonObject.toJSONString());
            producer.send(data);
        } else {
            _ackedNode.put(mapId, _jsonObject);
            if(ack){

            } else {
                ProducerRecord<String, String> data = new ProducerRecord<>("feed", _jsonObject.toJSONString());
                producer.send(data);
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}