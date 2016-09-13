package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.persistence.dto.PeerDTO;
import java.util.List;

public interface IPeerDAO {
    String toID(String roadMapID, String mapID);
    /**
     * Handles adding a peer,
     * adding a roadMapID
     * adding a mapID
     * adding a status
     * adding a payload
     *
     * @param dto
     */
    String addPeer(PeerDTO dto);

    /**
     * Handles verifying peer which returns peerID
     * Getting the current roadMapID
     * Getting the current mapID
     * Getting a list of status
     * Getting a list of payload
     *
     * @return
     */
    PeerDTO getPeer(String ID);

    List<PeerDTO> getAllPeers();

    /**
     * mostly be used for updating the device status relevant to current node
     *
     * @param dto
     */
    void updatePeer(PeerDTO dto);

    /**
     * Deletes all peers
     */
    void deleteAllPeers();

    /**
     * Deletes a peer
     */
    void deletePeer(String roadMapID, String mapID);
}
