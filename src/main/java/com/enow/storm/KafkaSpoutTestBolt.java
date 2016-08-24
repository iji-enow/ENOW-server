package com.enow.storm;

import org.apache.kafka.clients.producer.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaSpoutTestBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
    private OutputCollector collector;
    private Properties props;
    @Override
    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
		props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Override
    public void execute(Tuple input) {
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
    	final String msg = input.getValues().toString();
    	String word = msg.substring(1, msg.length() - 1);
//		String[] words = msg.split(" ");
//		for(String word:words){
			System.out.println("Word: " + word);
			collector.emit(new Values(word));
			ProducerRecord<String, String> data = new ProducerRecord<String, String>("onlytest", word);
			producer.send(data);
//		}
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
