package com.enow.facility;

import com.enow.daos.redisDAO.*;

/**
 * Created by writtic on 2016. 9. 12..
 */

public class DAOFacility implements IDAOFacility {
    private static IDAOFacility _instance;

    /**
     * Constructor
     */
    private DAOFacility(){
        
    }

    /**
     * Gets the instance of the DAOFacility
     * @return
     */
    public static IDAOFacility getInstance(){
        if(_instance == null)
            _instance = new DAOFacility();

        return _instance;
    }

    /**
     * Creates a new NodeDAO
     *
     * @return NodeDAO that implements INodeDAO
     */
    @Override
    public INodeDAO createNodeDAO() {
        return new NodeDAO();
    }

    /**
     * Creates a new StatusDAO
     *
     * @return StatusDAO that implements IStatusDAO
     */
    @Override
    public IStatusDAO createStatusDAO() {
        return new StatusDAO();
    }

    /**
     * Creates a new TerminateDAO
     *
     * @return StatusDAO that implements ITerminateDAO
     */
    @Override
    public ITerminateDAO createTerminateDAO() {
        return new TerminateDAO();
    }

}
