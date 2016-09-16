package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import java.util.*;

import com.enow.persistence.dto.NodeDTO;
import com.enow.persistence.redis.RedisDB;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;


public class NodeDAO implements INodeDAO {

    private static final String NODE_PREFIX = "node-";

    @Override
    public String toID(String roadMapID, String mapID) {
        String id = roadMapID + "-" + mapID;
        return id;
    }

    @Override
    public NodeDTO jsonObjectToNode(JSONObject jsonObject){

        String roadMapID = (String) jsonObject.get("roadMapId");
        String mapID = (String) jsonObject.get("mapId");
        String topic = (String) jsonObject.get("topic");
        JSONObject payload = (JSONObject) jsonObject.get("payload");
        NodeDTO dto = new NodeDTO(roadMapID, mapID, topic, payload.toJSONString());
        return dto;
    }

    @Override
    public String addNode(NodeDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        String id = dto.getRoadMapID() + "-" + dto.getMapID();

        Set<String> keys = jedis.keys("node-*");
        Iterator<String> iter = keys.iterator();
        ArrayList<String> ids = new ArrayList<>();

        boolean nodeExists = false;

        while(iter.hasNext()) {
            String key = iter.next();
            key = key.substring(5, key.length());
            ids.add(key);
            if(key.equals(id)) {
                nodeExists = true;
            }
        }
        if(!nodeExists) {
            jedis.lpush(NODE_PREFIX + id, dto.getTopic());
            jedis.lpush(NODE_PREFIX + id, dto.getPayload());
            return id;
        } else {
            jedis.del(NODE_PREFIX + id);
            jedis.lpush(NODE_PREFIX + id, dto.getTopic());
            jedis.lpush(NODE_PREFIX + id, dto.getPayload());
            return id + " overwrited";
        }
    }
    @Override
    public NodeDTO getNode(String ID) {
        Jedis jedis = RedisDB.getConnection();
        StringTokenizer tokenizer = new StringTokenizer(ID, "-");
        String roadMapID = tokenizer.nextToken();
        String mapID = tokenizer.nextToken();
        String id = roadMapID + "-" + mapID;
        List<String> result = jedis.lrange(NODE_PREFIX + id, 0, -1);
        if (result.size() > 1) {
            NodeDTO dto = new NodeDTO(roadMapID, mapID, result.get(0), result.get(1));
            return dto;
        } else {
            return null;
        }
    }

    @Override
    public List<NodeDTO> getAllNodes() {
        Jedis jedis = RedisDB.getConnection();
        List<NodeDTO> nodes = new ArrayList<>();
        Set<String> keys = jedis.keys("node-*");
        for (String key : keys) {
            key = key.substring(5, key.length());
            nodes.add(getNode(key));
        }
        return nodes;
    }

    @Override
    public void updateNode(NodeDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        String id = dto.getRoadMapID() + "-" + dto.getMapID();
        jedis.rpop(NODE_PREFIX + id);
        jedis.rpush(NODE_PREFIX + id, dto.getPayload());
    }
    @Override
    public void deleteAllNodes() {
        Jedis jedis = RedisDB.getConnection();
        Set<String> keys = jedis.keys("node-*");
        Iterator<String> iter = keys.iterator();
        while (iter.hasNext()) {
            jedis.del(iter.next());
        }
    }
    @Override
    public void deleteNode(String ID) {
        Jedis jedis = RedisDB.getConnection();
        jedis.del(NODE_PREFIX + ID);
    }
}
