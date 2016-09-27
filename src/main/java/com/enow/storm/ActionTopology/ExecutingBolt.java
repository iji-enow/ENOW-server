package com.enow.storm.ActionTopology;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.esotericsoftware.minlog.Log;

import java.util.Map;

public class ExecutingBolt extends ShellBolt implements IRichBolt {
    // Call the countbolt.py using Python
	protected static final Logger _LOG = LogManager.getLogger(ExecutingBolt.class);
    public ExecutingBolt() {
        super("python", "executingBolt.py");
    }
    
    @Override
    public void execute(Tuple input){
    	_LOG.fatal(input);
    	super.execute(input);
    }

    // Nothing to do for configuration
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("jsonObject"));
		
	}
}

/*
public class ExecutingBolt extends BaseRichBolt {
    // This Bolt is for Test
    protected static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    private OutputCollector _collector;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        JSONObject _jsonObject;
        _jsonObject = (JSONObject) input.getValueByField("jsonObject");
        Boolean verified = (Boolean) _jsonObject.get("verified");
        if(verified) {
            JSONObject _payload = new JSONObject();
            _payload.put(_jsonObject.get("mapId"), "great!");
            _jsonObject.put("payload", _payload);
        }

        _collector.emit(new Values(_jsonObject));

        try {
            _collector.ack(input);
        } catch (Exception e) {
        	Log.warn("ack failed");
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("jsonObject"));
    }
}
*/