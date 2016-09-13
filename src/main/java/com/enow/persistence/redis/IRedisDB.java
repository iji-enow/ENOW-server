package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.persistence.dto.NodeDTO;
import com.enow.persistence.dto.StatusDTO;

import java.util.List;

public interface IRedisDB {

    void clear();

    void shutdown();

    String addNode(NodeDTO dto);

    List<NodeDTO> getAllNodes();

    String addStatus(StatusDTO dto);

    List<StatusDTO> getAllStatus();
}
