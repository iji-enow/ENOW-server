package com.enow.facility;

import com.enow.daos.redisDAO.IPeerDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

public interface IDAOFacility {
    /**
     * Creates a new EnowRedisDAO
     *
     * @return EnowRedisDAO
     */
    IPeerDAO createPeerDAO();
}
