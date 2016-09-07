package com.enow.storm.TriggerTopology;

import java.util.Map;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;

// MongoDB

import com.enow.dto.TopicStructure;

public class IndexingBolt extends BaseRichBolt {
	protected static final Logger _LOG = LogManager.getLogger(IndexingBolt.class);
	private OutputCollector collector;

	@Override

	public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		if ((null == input.toString()) || (input.toString().length() == 0)) {
			return;
		}

		String temp = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

		
		JSONObject json = null;
        String webhook = null;
        Connect con = new Connect("https://hooks.slack.com/services/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");

        
        json = new JSONObject();
        json.put("text",input.getValues().toString().substring(1, input.getValues().toString().length() - 1));
        //json.put("text",elements[1].toString());
        //json.put("text",elements[2].toString());
        webhook = con.post(con.getURL(), json);
		
		TopicStructure _topicStructure = new TopicStructure();
		String[] elements = new String[3];
		String[] messages = new String[2];
		String[] topics = new String[8];
		StringTokenizer tokenizer;
		tokenizer = new StringTokenizer(temp, ",");
		
		
        
		for (int index = 0; tokenizer.hasMoreTokens(); index++) {
			elements[index] = tokenizer.nextToken().toString();
		}

		if (elements[0].equals("trigger")) {
			tokenizer = new StringTokenizer(elements[1], "/");
			for (int index = 0; tokenizer.hasMoreTokens(); index++) {
				topics[index] = tokenizer.nextToken().toString();
				if(topics[index] == null || (topics[index].length() == 0)){
					return;
				}
			}
			_topicStructure.setCorporationName(topics[0]);
			_topicStructure.setServerId(topics[1]);
			_topicStructure.setBrokerId(topics[2]);
			_topicStructure.setDeviceId(topics[3]);
			_topicStructure.setPhaseRoadMapId(topics[4]);
			
			tokenizer = new StringTokenizer(elements[2], "/");
			
			for (int index = 0; tokenizer.hasMoreTokens(); index++) {
				messages[index] = tokenizer.nextToken().toString();
			}
			
			_topicStructure.setCurrentMsg(messages[0]);
			
			if ((null == messages[0]) || (messages[0].length() == 0)) {
				return;
			}
			
		} else if (elements[0].equals("proceed")) {
			tokenizer = new StringTokenizer(elements[1], "/");
			for (int index = 0; tokenizer.hasMoreTokens(); index++) {
				topics[index] = tokenizer.nextToken().toString();
				if(topics[index] == null || (topics[index].length() == 0)){
					return;
				}
			}
			_topicStructure.setCorporationName(topics[0]);
			_topicStructure.setServerId(topics[1]);
			_topicStructure.setBrokerId(topics[2]);
			_topicStructure.setDeviceId(topics[3]);
			_topicStructure.setPhaseRoadMapId(topics[4]);
			_topicStructure.setPhaseId(topics[5]);
			_topicStructure.setCurrentMapId(topics[6]);
			_topicStructure.setPreviousMapId(topics[7]);

			tokenizer = new StringTokenizer(elements[2], "/");
			
			for (int index = 0; tokenizer.hasMoreTokens(); index++) {
				messages[index] = tokenizer.nextToken().toString();
			}
			_topicStructure.setCurrentMsg(messages[0]);	
			_topicStructure.setPreviousMsg(messages[1]);
			
			if (((null == messages[0]) || (messages[0].length() == 0)) || ((null == messages[1]) || (messages[0].length() == 1))) {
				return;
			}
		} else {

		}
		
        
        json = new JSONObject();
        json.put("text","여기 왓");
        //json.put("text",elements[1].toString());
        //json.put("text",elements[2].toString());
        webhook = con.post(con.getURL(), json);

		collector.emit(new Values(elements[0], _topicStructure));
		try {
			_LOG.debug("input = [" + input + "]");
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("spoutSource", "topicStructure"));
	}
}