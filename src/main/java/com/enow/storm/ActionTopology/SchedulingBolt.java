package com.enow.storm.ActionTopology;

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
import java.util.concurrent.ConcurrentHashMap;

public class SchedulingBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(SchedulingBolt.class);
    ConcurrentHashMap<String[], Boolean> _peerNode = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, JSONObject> _visitedNode = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, JSONObject> _previousData = new ConcurrentHashMap<>();
    private OutputCollector _collector;
    private JSONParser _parser;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _parser = new JSONParser();
    }

    @Override
    public void execute(Tuple input) {

        JSONObject _jsonObject;

        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }

        String msg = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

        try {
            _jsonObject = (JSONObject) _parser.parse(msg);
            _LOG.warn("Succeed in inserting messages to JSONObject : \n" + _jsonObject.toJSONString());
        } catch (ParseException e1) {
            e1.printStackTrace();
            _LOG.warn("Fail in inserting messages to JSONObject");
            _collector.fail(input);
            return;
        }

        System.out.println(_jsonObject.toJSONString());

        JSONArray waitingJSON = (JSONArray) _jsonObject.get("waitingPeer");
        JSONArray incomingJSON = (JSONArray) _jsonObject.get("incomingPeer");
        String[] waitingPeers = new String[waitingJSON.size()];
        String[] incomingPeers = new String[incomingJSON.size()];

        for (int i = 0; i < waitingJSON.size(); i++)
            waitingPeers[i] = (String) waitingJSON.get(i);
        for (int i = 0; i < incomingJSON.size(); i++)
            incomingPeers[i] = (String) incomingJSON.get(i);

        if ((Boolean) _jsonObject.get("ack")) {
            // Acknowledge ack = true
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
                    _peerNode.remove(waitingPeers);
                }
            }
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
