package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.enow.persistence.redis.RedisDB;
import com.enow.persistence.dto.PeerDTO;
import redis.clients.jedis.Jedis;


public class PeerDAO implements IPeerDAO {

    private static final String PEER_PREFIX = "peer-";

    @Override
    public int addPeer(PeerDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        Set<String> keys = jedis.keys("peer-*");
        Iterator<String> iter = keys.iterator();
        boolean peerExists = false;
        ArrayList<String> ids = new ArrayList<>();
        while(iter.hasNext()) {
            String key = iter.next();
            key = key.substring(5, key.length());
            ids.add(key);
            if(Integer.parseInt(key) == dto.getPeerID()) {
                peerExists = true;
            }
        }
        if(!peerExists) {
            jedis.lpush("peer-" + dto.getPeerID(), dto.getMapID());
            jedis.lpush("peer-" + dto.getPeerID(), dto.getState());
            jedis.lpush("peer-" + dto.getPeerID(), dto.getPayload());
            return dto.getPeerID();
        } else {
            int id = dto.getPeerID();
            while(ids.contains(id + "")) {
                id++;
            }
            jedis.lpush("peer-" + id, dto.getMapID());
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
            peers.add(getPeer(Integer.parseInt(key)));
        }
        return peers;
    }
    @Override
    public PeerDTO getPeer(int peerID) {
        Jedis jedis = RedisDB.getConnection();
        List<String> result = jedis.lrange(PEER_PREFIX + peerID, 0, 2);
        PeerDTO dto = new PeerDTO(peerID, result.get(0), result.get(1), result.get(2));
        return dto;
    }

    @Override
    public void updatePeer(PeerDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        jedis.rpop(PEER_PREFIX + dto.getPeerID());
        jedis.rpush(PEER_PREFIX + dto.getPeerID(), dto.getState());
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
    public void deletePeer(int peerID) {
        Jedis jedis = RedisDB.getConnection();
        jedis.del(PEER_PREFIX + peerID);
    }
}
