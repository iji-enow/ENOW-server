package com.enow.storm.TriggerTopology;


import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.text.SimpleDateFormat;
import java.util.Date;
import com.enow.dto.TopicStructure;

public class IndexingBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private OutputCollector collector;
    private TopicStructure topicStructure;

    @Override

    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        topicStructure = new TopicStructure();
    }

    @Override
    public void execute(Tuple input) {
        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }

        final String inputMsg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

        String topic = inputMsg.split(" ",2)[0];

        topicStructure.setCorporationName(topic.split("/")[0]);
        topicStructure.setServerId(topic.split("/")[1]);
        topicStructure.setBrokerId(topic.split("/")[2]);
        topicStructure.setDeviceId(topic.split("/")[3]);
        topicStructure.setPhaseRoadMapId(topic.split("/")[4]);

        // enow/serverId/brokerId/deviceId/phaseRoadMapId
        
        String msg = inputMsg.split(" ",2)[1];
        
        topicStructure.setMsg(msg);


        if ((null == inputMsg) || (inputMsg.length() == 0)) {
            return;
        }


        MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );

        mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        MongoDatabase dbWrite = mongoClient.getDatabase("enow");
        MongoCollection<Document> collection = dbWrite.getCollection("log");

        long time = System.currentTimeMillis();
        SimpleDateFormat dayTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Document document = new Document();
        document.put("time", dayTime.format(new Date(time)));
        document.put("topic", topic);
        document.put("msg", msg);

        collection.insertOne(document);

        mongoClient.close();

        collector.emit(new Values(topicStructure));
        try {
            LOG.debug("input = [" + input + "]");
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topicStructure"));
    }
}