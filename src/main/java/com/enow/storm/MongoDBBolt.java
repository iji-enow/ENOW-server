package com.enow.storm;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

import javax.imageio.ImageIO;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.shade.com.twitter.chill.Base64;
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

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
    private OutputCollector collector;
    @Override
    
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
    	
		Document packet = new Document();
	    if((null == word) || (word.length() == 0))
	    {
	        return;
	    }
	    //packet.put("picture",result);
	    
	    packet.put("word",word);
	    MongoClient mongoClient = new MongoClient( "127.0.0.1",27017 );
	    
	    //WriteConcern writeConcern=new WriteConcern();
	    mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
	    MongoDatabase db = mongoClient.getDatabase("word");
	    MongoCollection<Document> iotSampleColl = db.getCollection("word");
	    iotSampleColl.insertOne(packet);
	    mongoClient.close();
		
		collector.emit(new Values(word));
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
