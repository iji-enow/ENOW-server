package com.enow.storm.ActionTopology;

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.daos.redisDAO.IStatusDAO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.NodeDTO;
import com.enow.persistence.dto.StatusDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchedulingBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(SchedulingBolt.class);
    private INodeDAO _nodeDAO;
    private IStatusDAO _statusDAO;
    private OutputCollector _collector;
    private JSONParser _parser;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _nodeDAO = DAOFacility.getInstance().createNodeDAO();
        _statusDAO = DAOFacility.getInstance().createStatusDAO();
        _collector = collector;
        _parser = new JSONParser();
    }

    @Override
    public void execute(Tuple input) {

        _LOG.debug("Entering SchedulingBolt");

        JSONObject _jsonObject;

        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }
        // Parsing JSONString to JSONObject
        String jsonString = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);
        try {
            _jsonObject = (JSONObject) _parser.parse(jsonString);
            _LOG.debug("Succeed in inserting messages to _jsonObject : " + _jsonObject.toJSONString());
        } catch (ParseException e1) {
            e1.printStackTrace();
            _LOG.warn("Fail in inserting messages to _jsonObject");
            _collector.fail(input);
            return;
        }

        Boolean order = (Boolean) _jsonObject.get("order");
        String topic = (String) _jsonObject.get("topic");
        System.out.println("String topic = (String) _jsonObject.get(\"topic\") : " + topic);
        if(!order) {
            // Ready to get the status of device we need
            StatusDTO statusDTO = _statusDAO.getStatus(topic);
            String temp = statusDTO.getPayload();
            try {
                _jsonObject.put("payload", _parser.parse(temp));
                _LOG.debug("Succeed in inserting status to _jsonObject : " + _jsonObject.toJSONString());
            } catch (ParseException e1) {
                e1.printStackTrace();
                _LOG.warn("Fail in inserting status to _jsonObject");
                return;
            }
        }

        String roadMapId = (String) _jsonObject.get("roadMapId");
        JSONArray incomingJSON = (JSONArray) _jsonObject.get("incomingNode");
        String[] incomingNodes = null;
        if(incomingJSON != null){
            incomingNodes = new String[incomingJSON.size()];
            // If this node have incoming nodes...
            for (int i = 0; i < incomingJSON.size(); i++)
                incomingNodes[i] = (String) incomingJSON.get(i);
            // Put the previous data incoming nodes have in _jsonObject
            if(incomingNodes != null) {
                JSONObject tempJSON = new JSONObject();
                List<NodeDTO> checker = new ArrayList<>();
                for(String nodeId : incomingNodes) {
                    String id = _nodeDAO.toID(roadMapId, nodeId);
                    NodeDTO tempDTO = _nodeDAO.getNode(id);
                    if(tempDTO != null) {
                        checker.add(tempDTO);
                    }
                }

                if(checker.size() == incomingJSON.size()) {
                    for(String nodeId : incomingNodes) {
                        String id = _nodeDAO.toID(roadMapId, nodeId);
                        NodeDTO nodeDTO = _nodeDAO.getNode(id);
                        tempJSON.put(nodeId, nodeDTO.getPayload());
                        _jsonObject.put("previousData", tempJSON);
                        _nodeDAO.deleteNode(id);
                    }
                } else {
                    _jsonObject.put("varified", false);
                }

                _jsonObject.put("previousData", tempJSON);
                _LOG.debug("Succeed in inserting previousData to _jsonObject : " + tempJSON.toJSONString());
            }
        }
        // Store this node for subsequent node
        String result = "nothing";
        try {
            NodeDTO dto = _nodeDAO.jsonObjectToNode(_jsonObject);
            result = _nodeDAO.addNode(dto);
            _LOG.warn("Succeed in inserting current node to Redis : " + result);
        } catch(Exception e) {
            e.printStackTrace();
            _LOG.warn("Fail in inserting current node to Redis : " + result);
        }

        _collector.emit(new Values(_jsonObject));
        try {
            _LOG.debug("input = [" + input + "]");
            _collector.ack(input);
        } catch (Exception e) {
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("jsonObject"));
    }
}
