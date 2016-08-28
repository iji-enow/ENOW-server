package com.enow.storm.TriggerTopology;

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

public class StagingBolt extends BaseRichBolt {
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
    	
    	if(null == input.getValueByField("topic"))
 	    {
 	        return;
 	    }else if((null == input.getStringByField("msg") || input.getStringByField("msg").length() == 0))
 	    {
 	        return;
 	    }
    	
    	ts = (TopicStructure)input.getValueByField("topic");
    	final String msg = input.getStringByField("msg");
    
    	
    	
    	
    	
    	
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