package com.enow.storm.ActionTopology;
import org.apache.kafka.clients.producer.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import com.enow.dto.TopicStructure;

public class CallingFeedBolt extends BaseRichBolt {
    protected static final Logger _LOG = LoggerFactory.getLogger(CallingFeedBolt.class);
    private OutputCollector _collector;
	private TopicStructure _topicStructure;
	private String _spoutSource;
	private boolean _check;
    private Producer<String, String> producer;
	private Properties props;

    @Override
    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
		props = new Properties();
		props.put("producer.type", "sync");
		props.put("batch.size", "1");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer =  new KafkaProducer<String, String>(props);
		_topicStructure = new TopicStructure();
    }

    @Override
    public void execute(Tuple input) {
		if ((null == input.toString()) || (input.toString().length() == 0)) {
			_LOG.warn("input value or length of input is empty : [" + input + "]\n");
			return;
		}
    	_topicStructure = (TopicStructure) input.getValueByField("topicStructure");
		_spoutSource = (String) input.getValueByField("spoutSource");
		_check = (boolean) input.getValueByField("check");
		_collector.emit(new Values(_topicStructure));
		try {
			_LOG.debug("input = [" + input + "]");
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
		}
    	/*
    	if(null == input.getValueByField("topic"))
 	    {
 	        return;
 	    }else if((null == input.getStringByField("msg") || input.getStringByField("msg").length() == 0))
 	    {
 	        return;
 	    }
 	    */

    	//ts = (TopicStructure)input.getValueByField("topic");
    	//final String msg = input.getStringByField("msg");
		//ProducerRecord<String, String> data = new ProducerRecord<String, String>("feed", "ServerID: " + ts.getServerId() +  " msg : " + msg);
		if(_check) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>("feed", _topicStructure.output());
			producer.send(data);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}