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


public class IndexingBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private OutputCollector collector;
    private TopicStructure ts;
    @Override
    
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        ts = new TopicStructure();
    }

    @Override
    public void execute(Tuple input) {
    	if((null == input.toString()) || (input.toString().length() == 0))
 	    {
 	        return;
 	    }
    	
    	final String inputMsg = input.getValues().toString().substring(1,input.getValues().toString().length() - 1);	
    	
    	String topic = inputMsg.split(" ")[0];
    	
    	ts.setCorporationName(topic.split("/")[0]);
    	ts.setServerId(topic.split("/")[1]);
    	ts.setBrokerId(topic.split("/")[2]);
    	ts.setDeviceId(topic.split("/")[3]);
    	ts.setPhaseRoadMapId(topic.split("/")[4]);
    	
    	// enow/serverid/brokerid/deviceid/
    	
    	String msg = inputMsg.split(" ")[1];
    	
    	
	    if((null == inputMsg) || (inputMsg.length() == 0))
	    {
	        return;
	    }
	    
	    MongoClient mymongoClient = new MongoClient("127.0.0.1", 27017);

		mymongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase mydbWrite = mymongoClient.getDatabase("joon");
		MongoCollection<Document> mycollection = mydbWrite.getCollection("log");
		//mycollection.insertOne(new Document("topic", topic));
		//mycollection.insertOne(new Document("msg", msg));
		Document document = new Document();
		document.put("topic", topic);
		document.put("msg",msg);
		mycollection.insertOne(document);
		
		
		
		mymongoClient.close();
		
		collector.emit(new Values(ts,msg));
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topic","msg"));
    }
}