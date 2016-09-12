package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.persistence.dto.PeerDTO;

import java.util.List;

public interface IRedisDB {

    void clear();

    void shutdown();

    int addPeer(PeerDTO dto);

    List<PeerDTO> getAllPeers();
}
