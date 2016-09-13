package com.enow.facility;

import com.enow.daos.redisDAO.INodeDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

public interface IDAOFacility {
    /**
     * Creates a new NodeDAO
     *
     * @return NodeDAO
     */
    INodeDAO createPeerDAO();
}
