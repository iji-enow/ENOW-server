package com.enow.daos.redisDAO;

/**
 * Created by writtic on 2016. 9. 12..
 */

import com.enow.persistence.dto.NodeDTO;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.List;

public interface INodeDAO {
    void setJedisConnection(Jedis jedis);
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
     * adding payload
     * adding reference value by outingNodes
     *
     * @param dto
     */
    String addNode(NodeDTO dto);

    /**
     * Handles verifying node which returns nodeID
     * Getting the current roadMapID
     * Getting the current mapID
     * Getting a list of data
     * Create the reference value for checking incomming nodes
     *
     * @return dto
     */
    NodeDTO getNode(String ID);
    /*
     * Getting all nodes in redis
     */
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
