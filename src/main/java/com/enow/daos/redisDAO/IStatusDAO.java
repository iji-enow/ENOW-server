package com.enow.daos.redisDAO;

import com.enow.persistence.dto.StatusDTO;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by writtic on 2016. 9. 13..
 */
public interface IStatusDAO {
    void setJedisConnection(Jedis jedis);
    /**
     * change jsonObject to StatusDTO,
     *
     * @param jsonObject
     */
    StatusDTO jsonObjectToStatus(JSONObject jsonObject);
    /**
     * Handles adding a status,
     * adding a topic
     * adding a payload
     *
     * @param dto
     */
    String addStatus(StatusDTO dto);

    /**
     * Handles verifying status with topic which returns statusID
     * Getting the current payload
     *
     * @param topic
     * @return allStatus
     */
    StatusDTO getStatus(String topic);

    /**
     * Getting All payload stored
     *
     * @return allStatus
     */
    List<StatusDTO> getAllStatus();

    /**
     * mostly be used for updating the devices status
     *
     * @param dto
     */
    void updateStatus(StatusDTO dto);

    /**
     * Deletes all status
     */
    void deleteAllStatus();

    /**
     * Deletes 'a' status
     *
     * @param topic
     */
    void deleteStatus(String topic);
}
