package com.enow.storm.ActionTopology;

import com.enow.dto.TopicStructure;
import org.apache.logging.log4j.LogManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class ExecutingBolt extends ShellBolt implements IRichBolt {
    // Call the countbolt.py using Python
    public ExecutingBolt() {
        super("python", "executingBolt.py");
    }

    // Declare that we emit a 'jsonObject'
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("jsonObject", "message"));
    }

    // Nothing to do for configuration
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}