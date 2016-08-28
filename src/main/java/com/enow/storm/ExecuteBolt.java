package com.enow.storm;

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


import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ExecuteBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
	private OutputCollector collector;
    
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    

    public void execute(Tuple input) {
    	final String msg = input.getValues().toString();
    	String word = msg.substring(1, msg.length() - 1);
    	/*
    	String function = word.substring(1,word.length());
    	function = function.substring(10,function.length());
    	word = word.substring(0,1);
    	*/
    	
    	/*
    	String msgWord = input.getStringByField("word");
    	String msgFunction = input.getStringByField("function");
    	
    	String word = msgWord.substring(1, msgWord.length() - 1);
    	String function = msgFunction.substring(1, msgFunction.length() - 1);
    	*/
    	
		//String tmp = "word = " + word + "result = " + result + "function = " + function;
		
		
	    if((null == word) || (word.length() == 0))
	    {
	        return;
	    }
	    
	    /*
	    MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );
	  
	    mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
	    MongoDatabase db = mongoClient.getDatabase("word");
	    MongoCollection<Document> collection = db.getCollection("word");
	    */
	    //collection.insertOne(new Document("execute word",word));
	    //collection.insertOne(new Document("execute function",function));
	    //collection.insertOne(new Document("execute result",result));
	    
	    
	    /*
	    JsonObject json = null;
        String webhook = null;
        Connect con = new Connect("https://hooks.slack.com/services/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");

        
        json = new JsonObject();
        json.addProperty("text",word);
        webhook = con.post(con.getURL(), json);
	    */
	    //mongoClient.close();
		
		collector.emit(new Values(word));
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}	
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("word"));
    }
}