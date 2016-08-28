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

import com.enow.storm.TriggerTopology.TopicStructure;


public class ExecuteCodeBolt extends BaseRichBolt {
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

		
		collector.emit(new Values(inputMsg));
		try {
			LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("msg"));
    }
}