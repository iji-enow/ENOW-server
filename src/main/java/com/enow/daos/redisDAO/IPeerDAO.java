package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.persistence.dto.PeerDTO;
import java.util.List;

public interface IPeerDAO {
    /**
     * Handles adding a peer,
     * adding a mapID
     * adding a status
     * adding a payload
     *
     * @param dto
     */
    int addPeer(PeerDTO dto);

    /**
     * Handles verifying peer which returns peerID
     * Getting the current mapID
     * Getting a list of status
     * Getting a list of payload
     *
     * @return
     */
    PeerDTO getPeer(int peerID);

    List<PeerDTO> getAllPeers();

    /**
     * mostly be used for updating the game blob state
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
    void deletePeer(int peerID);
}
