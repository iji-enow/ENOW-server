package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.daos.redisDAO.INodeDAO;
import com.enow.daos.redisDAO.IStatusDAO;
import com.enow.persistence.dto.NodeDTO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.StatusDTO;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RedisDB implements IRedisDB {

    private static RedisDB instance;
    private static Jedis connection;

    public static RedisDB getInstance() {
        if(instance == null) {
            instance = new RedisDB();
        }
        return instance;
    }

    public static Jedis getConnection() {
        return getInstance().connection;
    }

    public RedisDB() {
        //connection = new Jedis("192.168.99.100", 6379);
        connection = new Jedis("127.0.0.1", 6379);
    }

    // public void setTestDb() {connection.select(10);}

    @Override
    public void clear() {
        Set<String> keys = connection.keys("*");
        Iterator<String> iter = keys.iterator();
        while(iter.hasNext()) {
            connection.del(iter.next());
        }
    }

    @Override
    public void shutdown() {
        connection.close();
    }

    @Override
    public String toID(String roadMapID, String nodeID) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.toID(roadMapID, nodeID);
    }

    @Override
    public NodeDTO jsonObjectToNode(JSONObject jsonObject){
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.jsonObjectToNode(jsonObject);
    }
    @Override
    public String addNode(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.addNode(dto);
    }

    @Override
    public NodeDTO getNode(String ID){
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.getNode(ID);
    }

    @Override
    public List<NodeDTO> getAllNodes() {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.getAllNodes();
    }

    @Override
    public void updateNode(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.updateNode(dto);
    }

    @Override
    public void updateRefer(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.updateRefer(dto);
    }

    @Override
    public void deleteNode(String ID) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.deleteNode(ID);
    }

    @Override
    public void deleteAllNodes() {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.deleteAllNodes();
    }

    @Override
    public StatusDTO jsonObjectToStatus(JSONObject jsonObject) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        return dao.jsonObjectToStatus(jsonObject);
    }

    @Override
    public String addStatus(StatusDTO dto) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        return dao.addStatus(dto);
    }

    @Override
    public StatusDTO getStatus(String topic) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        return dao.getStatus(topic);
    }

    @Override
    public List<StatusDTO> getAllStatus() {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        return dao.getAllStatus();
    }

    @Override
    public void updateStatus(StatusDTO dto) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.updateStatus(dto);
    }

    @Override
    public void deleteStatus(String topic) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.deleteStatus(topic);
    }

    @Override
    public void deleteAllStatus() {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.deleteAllStatus();
    }
}
