package com.enow.plugin;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.daos.redisDAO.INodeDAO;
import com.enow.facility.DAOFacility;
import com.enow.facility.IDAOFacility;

/**
 * Created by Kyle 'TMD' Cornelison on 4/2/2016.
 * <p>
 * Persistence Plugin based on Redis
 */
public class Plugin implements IRedisPlugin {
    private IDAOFacility factory = DAOFacility.getInstance();

    /**
     * Constructor
     */
    public Plugin() {
        // TODO: 4/2/2016 Set up the plugin, eg: database, etc.
    }

    /**
     * Returns a connection to the database
     *
     * @return
     */
    @Override
    public Object getConnection() {
        return null;
    }

    /**
     * Clears the database
     */
    @Override
    public void clear() {

    }

    /**
     * Starts a transaction on the database
     */
    @Override
    public void startTransaction() {

    }

    /**
     * Ends a transaction on the database
     *
     * @param commitTransaction
     */
    @Override
    public void endTransaction(boolean commitTransaction) {

    }

    /**
     * Creates a new NodeDAO
     *
     * @return NodeDAO
     */
    @Override
    public INodeDAO createPeerDAO() {
        return factory.createPeerDAO();
    }

}
