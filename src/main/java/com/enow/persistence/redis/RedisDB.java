package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.persistence.dto.PeerDTO;
import com.enow.daos.redisDAO.IPeerDAO;
import com.enow.facility.DAOFacility;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RedisDB implements IRedisDB {

    private static RedisDB instance;
    private static Jedis connection;

    public static RedisDB getInstance() {
        if(instance == null) {
            instance = new RedisDB();
        }
        return instance;
    }

    public static Jedis getConnection() {
        return getInstance().connection;
    }

    public RedisDB() {
        connection = new Jedis("127.0.0.1", 6379);
    }

    // public void setTestDb() {connection.select(10);}

    @Override
    public void clear() {
        Set<String> keys = connection.keys("*");
        Iterator<String> iter = keys.iterator();
        while(iter.hasNext()) {
            connection.del(iter.next());
        }
    }

    @Override
    public void shutdown() {
        connection.close();
    }

    @Override
    public int addPeer(PeerDTO dto) {
        IPeerDAO dao = DAOFacility.getInstance().createPeerDAO();
        return dao.addPeer(dto);
    }

    @Override
    public List<PeerDTO> getAllPeers() {
        IPeerDAO dao = DAOFacility.getInstance().createPeerDAO();
        return dao.getAllPeers();
    }
}
