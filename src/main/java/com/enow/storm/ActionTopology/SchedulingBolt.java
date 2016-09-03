package com.enow.storm.ActionTopology;

import java.util.Date;
import java.util.Map;

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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enow.dto.TopicStructure;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SchedulingBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(CallingFeedBolt.class);
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

        String inputStr = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
  
        String spoutName = inputStr.split(" ",3)[0];
        String topic = inputStr.split(" ",3)[1];
        String msg = inputStr.split(" ",3)[2];
        
        topicStructure.setMsg(msg);
        
        MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );

        mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        MongoDatabase dbWrite = mongoClient.getDatabase("enow");
        MongoCollection<Document> collection = dbWrite.getCollection("log");
        
        Document document = new Document();
        document.put("spoutName", spoutName);

        collection.insertOne(document);

        // enow/serverId/brokerId/deviceId/phaseRoadMapId/mapId
        topicStructure.setCorporationName(topic.split("/")[0]);
        topicStructure.setServerId(topic.split("/")[1]);
        topicStructure.setBrokerId(topic.split("/")[2]);
        topicStructure.setDeviceId(topic.split("/")[3]);
        topicStructure.setPhaseRoadMapId(topic.split("/")[4]);
        topicStructure.setMapId(topic.split("/")[5]);

        if (spoutName.equals("trigger")) {
            // trigger enow/serverId/brokerId/deviceId/phaseRoadMapId/mapId
            if (null == topicStructure) {
                return;
            }
            else{
            	 collector.emit(new Values(topicStructure));
                 
            	 try {
                     LOG.debug("input = [" + input + "]");
                     collector.ack(input);
                 } catch (Exception e) {
                     collector.fail(input);
                 }
            }
        } else if (spoutName.equals("status")) {
        	if (null == topicStructure) {
                return;
            }
            else{
            	 collector.emit(new Values(topicStructure));
                 
            	 try {
                     LOG.debug("input = [" + input + "]");
                     collector.ack(input);
                 } catch (Exception e) {
                     collector.fail(input);
                 }
            }
        } else if (spoutName.equals("proceed")){
//            try {
//                // status enow/serverId/brokerId/deviceId/phaseRoadMapId/mapId
//                JSONParser jsonParser = new JSONParser();
//                JSONObject json = (JSONObject) jsonParser.parse(msg);
//                String deviceStatus = json.get("status").toString();
//                String metadata = json.get("metadata").toString();
//
//            }catch(ParseException e){
//
//            }
        	/*
        	
        	if (null == topicStructure) {
                return;
            }
            else{
            	 collector.emit(new Values(topicStructure));
                 
            	 try {
                     LOG.debug("input = [" + input + "]");
                     collector.ack(input);
                 } catch (Exception e) {
                     collector.fail(input);
                 }
            }
            */
        }
        
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topicStructure"));
	}
}
