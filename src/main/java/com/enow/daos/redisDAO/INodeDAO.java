package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.persistence.dto.NodeDTO;
import org.json.simple.JSONObject;

import java.util.List;

public interface INodeDAO {
    String toID(String roadMapID, String mapID);
    /**
     * change jsonObject to NodeDTO,
     *
     * @param jsonObject
     */
    NodeDTO jsonObjectToNode(JSONObject jsonObject);
    /**
     * Handles adding a node,
     * adding a roadMapID
     * adding a mapID
     * adding a topic
     * adding data
     *
     * @param dto
     */
    String addNode(NodeDTO dto);

    /**
     * Handles verifying node which returns nodeID
     * Getting the current roadMapID
     * Getting the current mapID
     * Getting a list of topic
     * Getting a list of data
     *
     * @return
     */
    NodeDTO getNode(String ID);

    List<NodeDTO> getAllNodes();

    /**
     * mostly be used for updating the peer relevant to current node
     *
     * @param dto
     */
    void updateNode(NodeDTO dto);
    /**
     * updating refer valuable
     *
     * @param dto
     */
    void updateRefer(NodeDTO dto);
    /**
     * Deletes all nodes
     */
    void deleteAllNodes();

    /**
     * Deletes a node
     */
    void deleteNode(String ID);
}
