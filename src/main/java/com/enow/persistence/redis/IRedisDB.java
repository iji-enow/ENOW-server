package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.persistence.dto.NodeDTO;
import com.enow.persistence.dto.StatusDTO;
import com.enow.persistence.dto.TerminateDTO;
import org.json.simple.JSONObject;

import java.util.List;

public interface IRedisDB {

    void clear();
    void shutdown();

    String toID(String roadMapID, String mapID);
    NodeDTO jsonObjectToNode(JSONObject jsonObject);
    String addNode(NodeDTO dto);
    NodeDTO getNode(String ID);
    List<NodeDTO> getAllNodes();
    void updateNode(NodeDTO dto);
    void updateRefer(NodeDTO dto);
    void deleteNode(String ID);
    void deleteAllNodes();
    void deleteLastNode(NodeDTO dto);

    StatusDTO jsonObjectToStatus(JSONObject jsonObject);
    StatusDTO getStatus(String topic);
    String addStatus(StatusDTO dto);
    List<StatusDTO> getAllStatus();
    void updateStatus(StatusDTO dto);
    void deleteStatus(String topic);
    void deleteAllStatus();

    String addTerminate(String roadMapID);
    boolean isTerminate(String roadMapID);
    void deleteAllTerminate();
    void deleteTerminate(String roadMapID);
}
