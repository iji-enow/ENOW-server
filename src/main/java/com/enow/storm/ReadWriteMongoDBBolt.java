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

import org.json.simple.JSONValue;
import org.json.simple.JSONObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ReadWriteMongoDBBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
	private OutputCollector collector;
	private String tmp = "no";

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		final String msg = input.getValues().toString();
		String word = msg.substring(1, msg.length() - 1);

		if ((null == word) || (word.length() == 0)) {
			return;
		}

		/*
		MongoClient mymongoClient = new MongoClient("127.0.0.1", 27017);

		mymongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase mydbWrite = mymongoClient.getDatabase("word");
		MongoCollection<Document> mycollection = mydbWrite.getCollection("word");
		mycollection.insertOne(new Document("word", word));
		
		mymongoClient.close();
		*/
		
		
		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase dbWrite = mongoClient.getDatabase("source");
		MongoCollection<Document> collection = dbWrite.getCollection("codes");
			
		//FindIterable<Document> iterable = collection.find(new Document("name", new Document("$exists", true)));

		
		FindIterable<Document> iterable = collection.find(new Document("name", "zzz.txt"));

		String tmp =iterable.first().toJson();
		
		
		Object obj = JSONValue.parse(tmp);

	    JSONObject jsonMsg = (JSONObject)obj;

		//jsonmsg = new JSONObject(tmp);


		/*
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				tmp = document.toString();
			}
		});
		*/
		
		
		//String tmpyo = tmp.substring(61, tmp.length() - 2);
		
		JSONObject json = null;
        String webhook = null;
        Connect con = new Connect("https://hooks.slack.com/services/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");

        
        json = new JSONObject();
        json.put("text",tmp);
        webhook = con.post(con.getURL(), json);
		

		mongoClient.close();

		/*
		 * if(tmp.compareTo("function not found") == 0){
		 * 
		 * }else{ tmp = tmp.substring(40,tmp.length()-2); }
		 * 
		 * word = word + tmp;
		 */

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