package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import java.util.*;

import com.enow.persistence.dto.NodeDTO;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;


public class NodeDAO implements INodeDAO {

    private Jedis _jedis;

    private static final String NODE_PREFIX = "node-";

    @Override
    public void setJedisConnection(Jedis jedis) {
        _jedis = jedis;
    }

    @Override
    public String toID(String roadMapID, String nodeID) {
        String id = roadMapID + "-" + nodeID;
        return id;
    }

    @Override
    public NodeDTO jsonObjectToNode(JSONObject jsonObject){
        try {
            String roadMapID = (String) jsonObject.get("roadMapId");
            String nodeID = (String) jsonObject.get("nodeId");
            String topic = (String) jsonObject.get("topic");
            JSONArray outingJSON = (JSONArray) jsonObject.get("outingNode");
            Integer refer = 0;
            if (outingJSON != null) {
                refer = outingJSON.size();
            }
            JSONObject payload = (JSONObject) jsonObject.get("payload");
            NodeDTO dto;
            if (payload != null) {
                dto = new NodeDTO(roadMapID, nodeID, topic, payload.toJSONString(), "" + refer);
            } else {
                dto = new NodeDTO(roadMapID, nodeID, topic, "null", "" + refer);
            }
            return dto;
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public String addNode(NodeDTO dto) {
        try {
            String id = dto.getRoadMapID() + "-" + dto.getNodeID();

            Set<String> keys = _jedis.keys("node-*");
            Iterator<String> iter = keys.iterator();
            ArrayList<String> ids = new ArrayList<>();

            boolean nodeExists = false;

            while (iter.hasNext()) {
                String key = iter.next();
                key = key.substring(5, key.length());
                ids.add(key);
                if (key.equals(id)) {
                    nodeExists = true;
                }
            }
            if (!nodeExists) {
                _jedis.lpush(NODE_PREFIX + id, dto.getTopic());
                _jedis.lpush(NODE_PREFIX + id, dto.getPayload());
                _jedis.lpush(NODE_PREFIX + id, dto.getRefer());
                return id;
            } else {
                _jedis.del(NODE_PREFIX + id);
                _jedis.lpush(NODE_PREFIX + id, dto.getTopic());
                _jedis.lpush(NODE_PREFIX + id, dto.getPayload());
                _jedis.lpush(NODE_PREFIX + id, dto.getRefer());
                return id + " overwritten";
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }
    @Override
    public NodeDTO getNode(String ID) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(ID, "-");
            String roadMapID = tokenizer.nextToken();
            String nodeID = tokenizer.nextToken();
            String id = roadMapID + "-" + nodeID;
            List<String> result = _jedis.lrange(NODE_PREFIX + id, 0, -1);
            if (result.size() > 2) {
                NodeDTO dto = new NodeDTO(roadMapID, nodeID, result.get(2), result.get(1), result.get(0));
                return dto;
            } else {
                return null;
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public List<NodeDTO> getAllNodes() {
        try {
            List<NodeDTO> nodes = new ArrayList<>();
            Set<String> keys = _jedis.keys("node-*");
            for (String key : keys) {
                key = key.substring(5, key.length());
                nodes.add(getNode(key));
            }
            return nodes;
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void updateNode(NodeDTO dto) {
        try {
            String id = dto.getRoadMapID() + "-" + dto.getNodeID();
            _jedis.lpop(NODE_PREFIX + id);
            _jedis.lpop(NODE_PREFIX + id);
            _jedis.lpush(NODE_PREFIX + id, dto.getPayload());
            _jedis.lpush(NODE_PREFIX + id, dto.getRefer());
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void updateRefer(NodeDTO dto) {
        try {
            String id = dto.getRoadMapID() + "-" + dto.getNodeID();
            Integer refer = Integer.parseInt(dto.getRefer());
            --refer;
            if (refer > 0) {
                _jedis.lpop(NODE_PREFIX + id);
                _jedis.lpush(NODE_PREFIX + id, "" + refer);
            } else {
                _jedis.del(NODE_PREFIX + id);
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void deleteAllNodes() {
        try {
            Set<String> keys = _jedis.keys("node-*");
            Iterator<String> iter = keys.iterator();
            while (iter.hasNext()) {
                _jedis.del(iter.next());
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void deleteNode(String ID) {
        try {
            Set<String> keys = _jedis.keys("node-" + ID);
            Iterator<String> iter = keys.iterator();
            while (iter.hasNext()) {
                _jedis.del(iter.next());
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void deleteLastNode(NodeDTO dto) {
        try {
            String id = dto.getRoadMapID() + "-" + dto.getNodeID();
            _jedis.del(NODE_PREFIX + id);
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }
}
