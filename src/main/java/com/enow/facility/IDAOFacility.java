package com.enow.facility;

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.daos.redisDAO.IStatusDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

public interface IDAOFacility {
    /**
     * Creates a new NodeDAO
     *
     * @return NodeDAO
     */
    INodeDAO createNodeDAO();
    /**
     * Creates a new StatusDAO
     *
     * @return StatusDAO
     */
    IStatusDAO createStatusDAO();
}
