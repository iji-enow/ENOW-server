package com.enow.storm;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class PythonBolt extends ShellBolt implements IRichBolt {
	// Call the countbolt.py using Python
	public PythonBolt() {
		super("python", "examplebolt.py");
	}
	
	// Declare that we emit a 'word' and 'count'
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	// Nothing to do for configuration
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}