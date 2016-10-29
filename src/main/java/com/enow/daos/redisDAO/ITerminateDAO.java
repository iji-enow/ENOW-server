package com.enow.daos.redisDAO;

import com.enow.persistence.dto.TerminateDTO;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by writtic on 2016. 10. 29..
 */
public interface ITerminateDAO {
    void setJedisConnection(Jedis jedis);

    /**
     * Handles adding a status,
     * adding a topic
     * adding a payload
     *
     * @param roadMapID
     */
    String addTerminate(String roadMapID);

    /**
     * Handles verifying status with topic which returns statusID
     * Getting the current payload
     *
     * @param roadMapID
     * @return allTerminate
     */
    boolean isTerminate(String roadMapID);

    /**
     * Deletes all status
     */
    void deleteAllTerminate();

    /**
     * Deletes 'a' status
     *
     * @param roadMapID
     */
    void deleteTerminate(String roadMapID);
}
