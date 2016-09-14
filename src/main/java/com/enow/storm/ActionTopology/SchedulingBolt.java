package com.enow.storm.ActionTopology;

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.NodeDTO;
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

import java.util.Map;

public class SchedulingBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(SchedulingBolt.class);
    private INodeDAO _dao;
    private OutputCollector _collector;
    private JSONParser _parser;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _dao = DAOFacility.getInstance().createNodeDAO();
        _collector = collector;
        _parser = new JSONParser();
    }

    @Override
    public void execute(Tuple input) {

        _LOG.info("Entering SchedulingBolt");

        JSONObject _jsonObject;
        JSONObject _status;

        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }



        /*
        String jsonString = input.getStringByField("jsonObject").toString().substring(1, input.getValues().toString().length() - 1);
        if(source.equals("trigger-spout")) {
            try {
                _jsonObject = (JSONObject) _parser.parse(jsonString);
                _LOG.info("Succeed in inserting messages to _jsonObject : \n" + _jsonObject.toJSONString());
            } catch (ParseException e1) {
                e1.printStackTrace();
                _LOG.warn("Fail in inserting messages to _jsonObject");
                _collector.fail(input);
                return;
            }

            String roadMapId = (String) _jsonObject.get("roadMapId");
            String mapId = (String) _jsonObject.get("mapId");
            String corpName = (String) _jsonObject.get("corporationName");
            String serverId = (String) _jsonObject.get("serverId");
            String brokerId = (String) _jsonObject.get("brokerId");
            String deviceId = (String) _jsonObject.get("deviceId");
            String topic = corpName + "/" + serverId + "/" + brokerId + "/" + deviceId;

            JSONArray incomingJSON = (JSONArray) _jsonObject.get("incomingNode");
            String[] incomingNodes = null;
            if(incomingJSON != null){
                incomingNodes = new String[incomingJSON.size()];
                for (int i = 0; i < incomingJSON.size(); i++)
                    incomingNodes[i] = (String) incomingJSON.get(i);
            }
            if(incomingNodes != null) {
                JSONObject tmp = new JSONObject();
                for(String peerId : incomingNodes) {
                    String id = _dao.toID(roadMapId, peerId);
                    NodeDTO dto = _dao.getNode(id);

                }
                _jsonObject.put("previousData", tmp);
            }
            _collector.emit(new Values(_jsonObject));
            try {
                _LOG.debug("input = [" + input + "]");
                _collector.ack(input);
            } catch (Exception e) {
                _collector.fail(input);
            }
        } else if(source.equals("status-spout")){
            try {
                _status = (JSONObject) _parser.parse(jsonString);
                _LOG.info("Succeed in inserting messages to _status : \n" + _status.toJSONString());
            } catch (ParseException e1) {
                e1.printStackTrace();
                _LOG.warn("Fail in inserting messages to _status");
                _collector.fail(input);
                return;
            }
            System.out.println("_status: " + _status.toJSONString());

            _collector.emit(new Values(input));
            try {
                _LOG.debug("input = [" + input + "]");
                _collector.ack(input);
            } catch (Exception e) {
                _collector.fail(input);
            }

        } else {
            _LOG.warn("Fail in recieving the messages from Source Component");
            return;
        }
        */

        /*
        if ((Boolean) _jsonObject.get("ack")) {
            // Acknowledge ack = true
            _LOG.info("Acknowledge ack = true");
            Boolean proceed = false;
            if (waitingPeers != null) {
                if (_peerNode.containsKey(waitingPeers))
                    for (String peer : waitingPeers)
                        if (_previousData.containsKey(peer))
                            proceed = true;
                if (!proceed) {
                    String mapId = (String) _jsonObject.get("mapId");
                    JSONObject message = (JSONObject) _jsonObject.get("message");
                    _previousData.put(mapId, message);
                }
            } else _jsonObject.put("proceed", true);
        } else {
            // Execution Cycle : ack = false
            _LOG.info("Execution Cycle : ack = false");
            // 새로 들어온 `mapId`인지 확인
            String currentMapId = (String) _jsonObject.get("mapId");
            if (_visitedNode.containsKey(currentMapId)) {
                // 이미 방문했던 `mapId`
                _LOG.error("This node already visited before!"
                        + " Check the ConcurrentHashMap you created");
                return;
            } else {
                // 새로 방문한 `mapId`
                _visitedNode.put(currentMapId, _jsonObject);
                // 새로 들어온 `mapId`면 `ConcurrentHashMap`에 `peer`들과 함께 저장
                if (waitingPeers != null) { // waitingPeers exist
                    if ( _peerNode.containsKey(waitingPeers)) ;  // 이미 다른 peer로 부터 자신의 currentId가 저장됨
                    else _peerNode.put(waitingPeers, false);
                }
                if (incomingPeers != null) { // incomingPeers exist
                    JSONArray _jsonArray = new JSONArray();
                    int i = 0;
                    for (String peer : incomingPeers) {
                        _jsonArray.add(i++, _visitedNode.get(peer).get("message"));
                    }
                    _jsonObject.put("previousData", _jsonArray);
                    _peerNode.remove(incomingPeers);
                }
            }
        }
        */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("jsonObject", "status"));
    }
}
