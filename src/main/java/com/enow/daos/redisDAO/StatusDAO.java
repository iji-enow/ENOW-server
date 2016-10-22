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

    private Jedis _jedis;

    private static final String STATUS_PREFIX = "status-";

    @Override
    public void setJedisConnection(Jedis jedis) {
        _jedis = jedis;
    }

    @Override
    public StatusDTO jsonObjectToStatus(JSONObject jsonObject) {
        String topic = (String) jsonObject.get("topic");
        JSONObject payload = (JSONObject) jsonObject.get("payload");
        StatusDTO dto = new StatusDTO(topic, payload.toJSONString());
        return dto;
    }

    @Override
    public String addStatus(StatusDTO dto) {
        String id = dto.getTopic();

        Set<String> keys = _jedis.keys("status-*");
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
            _jedis.lpush("status-" + id, dto.getPayload());
            return id;
        } else {
            _jedis.del("status-" + id);
            _jedis.lpush("status-" + id, dto.getPayload());
            return id + " overwritten";
        }
    }

    @Override
    public StatusDTO getStatus(String topic) {
        List<String> result = _jedis.lrange(STATUS_PREFIX + topic, 0, 0);
        if (result.size() > 0) {
            StatusDTO dto = new StatusDTO(topic, result.get(0));
            return dto;
        } else {
            return null;
        }
    }

    @Override
    public List<StatusDTO> getAllStatus() {
        List<StatusDTO> allStatus = new ArrayList<>();
        Set<String> keys = _jedis.keys("status-*");
        for (String key : keys) {
            key = key.substring(5, key.length());
            allStatus.add(getStatus(key));
        }
        return allStatus;
    }

    @Override
    public void updateStatus(StatusDTO dto) {
        _jedis.rpop(STATUS_PREFIX + dto.getTopic());
        _jedis.rpush(STATUS_PREFIX + dto.getTopic(), dto.getPayload());
    }

    @Override
    public void deleteStatus(String topic) {
        _jedis.del(STATUS_PREFIX + topic);
    }

    @Override
    public void deleteAllStatus() {
        Set<String> keys = _jedis.keys("status-*");
        Iterator<String> iter = keys.iterator();
        while (iter.hasNext()) {
            _jedis.del(iter.next());
        }
    }
}
