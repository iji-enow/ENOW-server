package com.enow.plugin;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.daos.redisDAO.IStatusDAO;

/**
 * Created by Kyle 'TMD' Cornelison on 4/2/2016.
 */
public interface IRedisPlugin {
    //region Plugin Methods

    /**
     * Returns a connection to the database
     *
     * @return
     */
    Object getConnection(); // TODO: 12/9/2016 Maybe use a wrapper instead of Object...

    /**
     * Clears the database
     */
    void clear();

    /**
     * Starts a transaction on the database
     */
    void startTransaction();

    /**
     * Ends a transaction on the database
     *
     * @param commitTransaction
     */
    void endTransaction(boolean commitTransaction);

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
    //endregion
}
