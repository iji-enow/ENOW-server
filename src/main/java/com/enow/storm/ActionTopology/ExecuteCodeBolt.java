package com.enow.storm.ActionTopology;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enow.dto.TopicStructure;


public class ExecuteCodeBolt extends BaseRichBolt {
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
    	topicStructure = (TopicStructure) input.getValueByField("topicStructure");
		if (null == topicStructure) {
			return;
		}
    
		
		collector.emit(new Values(topicStructure));
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("topicStructure"));
    }
}