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

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;

public class StagingBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private OutputCollector collector;
    private TopicStructure ts;
	String machineIdCheck = null;
	String phaseRoadMapIdCheck = null;
    @Override
    
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        ts = new TopicStructure();
    }

    @Override
    public void execute(Tuple input) {
    	
    	if(null == input.getValueByField("topic"))
 	    {
 	        return;
 	    }else if((null == input.getStringByField("msg") || input.getStringByField("msg").length() == 0))
 	    {
 	        return;
 	    }
    	
    	ts = (TopicStructure)input.getValueByField("topic");
    	final String msg = input.getStringByField("msg");
    	
    	MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase dbWrite = mongoClient.getDatabase("joon");
		MongoCollection<Document> deviceListCollection = dbWrite.getCollection("deviceList");
		
		if(deviceListCollection.count(new Document("deviceId", ts.getDeviceId())) == 0){
			machineIdCheck = "device unchecked";
		}else if(deviceListCollection.count(new Document("deviceId", ts.getDeviceId())) == 1){
			machineIdCheck = "device checked";
		}else{
			machineIdCheck = "device id : now we have a problem";
		}
		
		MongoCollection<Document> phaseRoadMapCollection = dbWrite.getCollection("phaseRoadMap");
		if(phaseRoadMapCollection.count(new Document("phaseRoadMapId", ts.getPhaseRoadMapId())) == 0){
			phaseRoadMapIdCheck = "Phase road map unchecked";
		}else if(phaseRoadMapCollection.count(new Document("deviceId", ts.getDeviceId())) == 1){
			phaseRoadMapIdCheck = "Phase road map checked";
		}else{
			phaseRoadMapIdCheck = "phase road map id : now we have a problem";
		}
		
		
		
		
    	
		//FindIterable<Document> iterable = collection.find(new Document("deviceId", ts.getDeviceId()));

		
		
		//String phaseInfo =iterable.first().toJson();
    	
		collector.emit(new Values(ts,msg,machineIdCheck,phaseRoadMapIdCheck));
		
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","msg","machineIdCheck","phaseRoadMapIdCheck"));
    }
}