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

public class ReadWriteMongoDBBolt extends BaseRichBolt {
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
	private OutputCollector collector;
	private String tmp;

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
		// packet.put("picture",result);

		MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		MongoDatabase dbWrite = mongoClient.getDatabase("word");
		MongoCollection<Document> collection = dbWrite.getCollection("word");
		collection.insertOne(new Document("word", word));

		/*
		 * FindIterable<Document> iterable = collection.find(new
		 * Document("function1", new Document("$exists",true)));
		 * 
		 * iterable.forEach(new Block<Document>() {
		 * 
		 * @Override public void apply(final Document document) { tmp =
		 * document.toString(); } });
		 */

		mongoClient.close();

		/*
		 * if(tmp.compareTo("function not found") == 0){
		 * 
		 * }else{ tmp = tmp.substring(40,tmp.length()-2); }
		 * 
		 * word = word + tmp;
		 */

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