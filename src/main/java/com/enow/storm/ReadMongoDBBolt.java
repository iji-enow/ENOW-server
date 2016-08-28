package com.enow.storm;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingConstants;

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

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ReadMongoDBBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
    private OutputCollector collector;
    private String tmp;
    @Override
    
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tmp = "test : ";
    }

    @Override
    public void execute(Tuple input) {
    	final String msg = input.getValues().toString();
    	String word = msg.substring(1, msg.length() - 1);
    	
    	/*
    	try{
    		BufferedImage myPicture = ImageIO.read(new File("/Users/LeeGunJoon/Desktop/test.png")); 	
    		ByteArrayOutputStream os = new ByteArrayOutputStream();
    	    OutputStream b64 = new Base64.OutputStream(os);
    	    ImageIO.write(myPicture, "png", b64);
    	    result = os.toString("UTF-8");
    	}catch(IOException e){
    		
    	}
    	*/
    	
    	/*
		String url = "mongodb://127.0.0.1:27017/test";
		String collectionName = "word";

		MongoMapper mapper = new SimpleMongoMapper()
			       .withFields("word",word);

		MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName, mapper);
    	 */
    	
		if((null == word) || (word.length() == 0))
	    {
	        return;
	    }
	    
	    MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );
	    
	    //WriteConcern writeConcern=new WriteConcern();
	    mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
	    MongoDatabase db = mongoClient.getDatabase("word");
	    MongoCollection<Document> collection = db.getCollection("word");
	    

	    /*
		List<Document> documents = (List<Document>) collection.find().into(
				new ArrayList<Document>());
 
		
               for(Document document : documents){
            	   	tmp = tmp + document.toString();
               }
        */
	    
	    /*
	    FindIterable<Document> iterable = db.getCollection("word").find();
	    
	    iterable.forEach(new Block<Document>() {
	        @Override
	        public void apply(final Document document) {
	            tmp = document.toString();
	        }
	    }); 
	    */
	    
	    FindIterable<Document> iterable = collection.find(new Document("function1", new Document("$exists",true)));
	
	    //tmp = iterable.toString();
	    
	    /*
	    iterable.forEach(new Block<Document>() {
	        @Override
	        public void apply(final Document document) {
	        	tmp = tmp + document.toString();
	        }
	    });
	    
	    tmp = tmp.substring(40,tmp.length()-2);
	    
	    */
	    mongoClient.close();
		
		collector.emit(new Values(tmp));
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("word"));
    }
}