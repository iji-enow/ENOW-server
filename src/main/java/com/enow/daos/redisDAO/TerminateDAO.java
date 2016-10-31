package com.enow.daos.redisDAO;

import com.enow.persistence.dto.TerminateDTO;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by writtic on 2016. 10. 29..
 */
public class TerminateDAO implements ITerminateDAO {
    private Jedis _jedis;

    private static final String TERMINATE_PREFIX = "terminate-";

    @Override
    public void setJedisConnection(Jedis jedis) {
        _jedis = jedis;
    }

    @Override
    public String addTerminate(String roadMapID) {
        try {
            String id = roadMapID;

            Set<String> keys = _jedis.keys("terminate-*");
            Iterator<String> iter = keys.iterator();
            ArrayList<String> ids = new ArrayList<>();

            boolean terminateExists = false;

            while (iter.hasNext()) {
                String key = iter.next();
                key = key.substring(7, key.length());
                ids.add(key);
                if (key.equals(id)) {
                    terminateExists = true;
                }
            }
            if (!terminateExists) {
                _jedis.lpush("terminate-" + id, id);
                return id;
            } else {
                _jedis.del("terminate-" + id);
                _jedis.lpush("terminate-" + id, id);
                return id + " overwritten";
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public boolean isTerminate(String roadMapID) {
        try {
            List<String> result = _jedis.lrange(TERMINATE_PREFIX + roadMapID, 0, 0);
            if (result.size() > 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void deleteTerminate(String roadMapID) {
        try {
            _jedis.del(TERMINATE_PREFIX + roadMapID);
        } finally {
            if (_jedis != null) {
                _jedis.close();
            }
        }
    }

    @Override
    public void deleteAllTerminate() {
        try {
            Set<String> keys = _jedis.keys("terminate-*");
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
}
