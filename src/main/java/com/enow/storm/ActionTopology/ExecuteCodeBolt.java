package com.enow.storm.ActionTopology;

import com.enow.dto.TopicStructure;
import org.apache.logging.log4j.LogManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ExecuteCodeBolt extends BaseRichBolt {
    protected static final org.apache.logging.log4j.Logger _LOG = LogManager.getLogger(ExecuteCodeBolt.class);
    private OutputCollector _collector;
    private TopicStructure _topicStructure;
    private String _spoutSource;
    private boolean _check;
    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
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
        _check = (boolean) input.getBooleanByField("check");
        if(_check) {
            // If this input is ack node

        } else {
            // If this input is exec node

        }
        _collector.emit(new Values(_spoutSource, _topicStructure, _check));
		try {
			_LOG.debug("input = [" + input + "]");
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("spoutSource", "topicStructure", "check"));
    }
}