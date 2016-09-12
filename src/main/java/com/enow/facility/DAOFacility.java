package com.enow.facility;

import com.enow.daos.redisDAO.IPeerDAO;
import com.enow.daos.redisDAO.PeerDAO;

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
     * Creates a new PeerDAO
     *
     * @return PeerDAO that implements IPeerDAO
     */
    @Override
    public IPeerDAO createPeerDAO() {
        return new PeerDAO();
    }
}
