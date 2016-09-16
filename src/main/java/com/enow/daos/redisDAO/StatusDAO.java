package com.enow.daos.redisDAO;

import com.enow.persistence.dto.StatusDTO;
import com.enow.persistence.redis.RedisDB;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by writtic on 2016. 9. 13..
 */
public class StatusDAO implements IStatusDAO {

    private static final String STATUS_PREFIX = "status-";


    @Override
    public StatusDTO jsonObjectToStatus(JSONObject jsonObject) {
        String topic = (String) jsonObject.get("topic");
        JSONObject payload = (JSONObject) jsonObject.get("payload");
        StatusDTO dto = new StatusDTO(topic, payload.toJSONString());
        return dto;
    }

    @Override
    public String addStatus(StatusDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        String id = dto.getTopic();

        Set<String> keys = jedis.keys("status-*");
        Iterator<String> iter = keys.iterator();
        ArrayList<String> ids = new ArrayList<>();

        boolean statusExists = false;

        while (iter.hasNext()) {
            String key = iter.next();
            key = key.substring(7, key.length());
            ids.add(key);
            if (key.equals(id)) {
                statusExists = true;
            }
        }
        if (!statusExists) {
            jedis.lpush("status-" + id, dto.getPayload());
            return id;
        } else {
            jedis.del("status-" + id);
            jedis.lpush("status-" + id, dto.getPayload());
            return id + " overwrited";
        }
    }

    @Override
    public StatusDTO getStatus(String topic) {
        Jedis jedis = RedisDB.getConnection();
        List<String> result = jedis.lrange(STATUS_PREFIX + topic, 0, 0);
        if (result.size() > 0) {
            StatusDTO dto = new StatusDTO(topic, result.get(0));
            return dto;
        } else {
            return null;
        }
    }

    @Override
    public List<StatusDTO> getAllStatus() {
        Jedis jedis = RedisDB.getConnection();
        List<StatusDTO> allStatus = new ArrayList<>();
        Set<String> keys = jedis.keys("status-*");
        for (String key : keys) {
            key = key.substring(5, key.length());
            allStatus.add(getStatus(key));
        }
        return allStatus;
    }

    @Override
    public void updateStatus(StatusDTO dto) {
        Jedis jedis = RedisDB.getConnection();
        jedis.rpop(STATUS_PREFIX + dto.getTopic());
        jedis.rpush(STATUS_PREFIX + dto.getTopic(), dto.getPayload());
    }

    @Override
    public void deleteAllStatus() {
        Jedis jedis = RedisDB.getConnection();
        Set<String> keys = jedis.keys("status-*");
        Iterator<String> iter = keys.iterator();
        while (iter.hasNext()) {
            jedis.del(iter.next());
        }
    }

    @Override
    public void deleteStatus(String topic) {
        Jedis jedis = RedisDB.getConnection();
        jedis.del(STATUS_PREFIX + topic);
    }
}
