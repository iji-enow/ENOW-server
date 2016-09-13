package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import java.util.*;

import com.enow.persistence.redis.RedisDB;
import com.enow.persistence.dto.PeerDTO;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;


public class PeerDAO implements IPeerDAO {

    private static final String PEER_PREFIX = "peer-";

    @Override
    public String toID(String roadMapID, String mapID) {
        String id = roadMapID + "-" + mapID;
        return id;
    }

    @Override
    public PeerDTO jsonObjectToPeer(JSONObject jsonObject){
        String roadMapID = (String) jsonObject.get("roadMapId");
        String mapID = (String) jsonObject.get("mapId");
        String[] payload = null;
        String status =

        JSONArray payloadJSON = (JSONArray) jsonObject.get("payload");
        if(payloadJSON != null){
            payload = new String[payloadJSON.size()];
            for (int i = 0; i < payloadJSON.size(); i++)
                payload[i] = (String) payloadJSON.get(i);
        }
    }

    @Override
    public String addPeer(PeerDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        String id = dto.getRoadMapID() + "-" + dto.getMapID();

        Set<String> keys = jedis.keys("peer-*");
        Iterator<String> iter = keys.iterator();
        ArrayList<String> ids = new ArrayList<>();

        boolean peerExists = false;

        while(iter.hasNext()) {
            String key = iter.next();
            key = key.substring(5, key.length());
            ids.add(key);
            if(key.equals(id)) {
                peerExists = true;
            }
        }
        if(!peerExists) {
            jedis.lpush("peer-" + id, dto.getState());
            jedis.lpush("peer-" + id, dto.getPayload());
            return id + " overwrited";
        } else {
            jedis.lpush("peer-" + id, dto.getState());
            jedis.lpush("peer-" + id, dto.getPayload());
            return id;
        }
    }
    @Override
    public List<PeerDTO> getAllPeers() {
        Jedis jedis = RedisDB.getConnection();
        List<PeerDTO> peers = new ArrayList<>();
        Set<String> keys = jedis.keys("peer-*");

        for (String key : keys) {
            key = key.substring(5, key.length());
            peers.add(getPeer(key));
        }
        return peers;
    }
    @Override
    public PeerDTO getPeer(String ID) {
        Jedis jedis = RedisDB.getConnection();
        StringTokenizer tokenizer = new StringTokenizer(ID, "-");
        String roadMapID = tokenizer.nextToken();
        String mapID = tokenizer.nextToken();
        String id = roadMapID + mapID;
        List<String> result = jedis.lrange(PEER_PREFIX + id, 0, 1);
        PeerDTO dto = new PeerDTO(roadMapID, mapID, result.get(1), result.get(2));
        return dto;
    }

    @Override
    public void updatePeer(PeerDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        String id = dto.getRoadMapID() + "-" + dto.getMapID();
        jedis.rpop(PEER_PREFIX + id);
        jedis.rpush(PEER_PREFIX + id, dto.getState());
        jedis.rpush(PEER_PREFIX + id, dto.getPayload());
    }
    @Override
    public void deleteAllPeers() {
        Jedis jedis = RedisDB.getConnection();
        Set<String> keys = jedis.keys("peer-*");
        Iterator<String> iter = keys.iterator();
        while (iter.hasNext()) {
            jedis.del(iter.next());
        }
    }
    @Override
    public void deletePeer(String roadMapID, String mapID) {
        Jedis jedis = RedisDB.getConnection();
        String id = roadMapID + "-" + mapID;
        jedis.del(PEER_PREFIX + id);
    }
}
