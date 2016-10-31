package com.enow.persistence.redis;

/**
 * Created by writtic on 2016. 9. 12..
 */
import com.enow.daos.redisDAO.INodeDAO;
import com.enow.daos.redisDAO.IStatusDAO;
import com.enow.daos.redisDAO.ITerminateDAO;
import com.enow.persistence.dto.NodeDTO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.StatusDTO;
import org.json.simple.JSONObject;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RedisDB implements IRedisDB {

    private static RedisDB instance;
    private static JedisPool connection;

    public RedisDB(String IP, int PORT) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10000);
        poolConfig.setMaxIdle(100);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        connection = new JedisPool(
                poolConfig,
                IP,
                PORT
        );
    }

    static public JedisPool getConnection(String IP, int PORT) {
        return getInstance(IP, PORT).connection;
    }

    static public RedisDB getInstance(String IP, int PORT) {
        if (instance == null) {
            instance = new RedisDB(IP, PORT);
        }
        return instance;
    }
    /*
    static public RedisDB getInstance(String IP, int PORT) {
        if(instance == null) {
            instance = new RedisDB(IP, PORT);
        }
        return instance;
    }

    static public Jedis getConnection(String IP, int PORT) {
        return getInstance(IP, PORT).connection;
    }

    public RedisDB(String IP, int PORT) {
        // connection = new Jedis("192.168.99.100", 6379);
        connection = new Jedis(IP, PORT);
    }
    */
    // public void setTestDb() {connection.select(10);}

    @Override
    public void clear() {
        Set<String> keys = connection.getResource().keys("*");
        Iterator<String> iter = keys.iterator();
        while(iter.hasNext()) {
            connection.getResource().del(iter.next());
        }
    }

    @Override
    public void shutdown() {
        connection.getResource().close();
    }

    @Override
    public String toID(String roadMapID, String nodeID) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        return dao.toID(roadMapID, nodeID);
    }

    @Override
    public NodeDTO jsonObjectToNode(JSONObject jsonObject){
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.jsonObjectToNode(jsonObject);
    }
    @Override
    public String addNode(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.addNode(dto);
    }

    @Override
    public NodeDTO getNode(String ID){
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.getNode(ID);
    }

    @Override
    public List<NodeDTO> getAllNodes() {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.getAllNodes();
    }

    @Override
    public void updateNode(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        dao.updateNode(dto);
    }

    @Override
    public void updateRefer(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        dao.updateRefer(dto);
    }

    @Override
    public void deleteNode(String ID) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteNode(ID);
    }

    @Override
    public void deleteAllNodes() {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteAllNodes();
    }

    @Override
    public void deleteLastNode(NodeDTO dto) {
        INodeDAO dao = DAOFacility.getInstance().createNodeDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteLastNode(dto);
    }

    @Override
    public StatusDTO jsonObjectToStatus(JSONObject jsonObject) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        return dao.jsonObjectToStatus(jsonObject);
    }

    @Override
    public String addStatus(StatusDTO dto) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.addStatus(dto);
    }

    @Override
    public StatusDTO getStatus(String topic) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.getStatus(topic);
    }

    @Override
    public List<StatusDTO> getAllStatus() {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.getAllStatus();
    }

    @Override
    public void updateStatus(StatusDTO dto) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        dao.updateStatus(dto);
    }

    @Override
    public void deleteStatus(String topic) {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteStatus(topic);
    }

    @Override
    public void deleteAllStatus() {
        IStatusDAO dao = DAOFacility.getInstance().createStatusDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteAllStatus();
    }

    @Override
    public String addTerminate(String roadMapID) {
        ITerminateDAO dao = DAOFacility.getInstance().createTerminateDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.addTerminate(roadMapID);
    }

    @Override
    public boolean isTerminate(String roadMapID) {
        ITerminateDAO dao = DAOFacility.getInstance().createTerminateDAO();
        dao.setJedisConnection(connection.getResource());
        return dao.isTerminate(roadMapID);
    }
    @Override
    public void deleteAllTerminate() {
        ITerminateDAO dao = DAOFacility.getInstance().createTerminateDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteAllTerminate();
    }
    @Override
    public void deleteTerminate(String roadMapID) {
        ITerminateDAO dao = DAOFacility.getInstance().createTerminateDAO();
        dao.setJedisConnection(connection.getResource());
        dao.deleteTerminate(roadMapID);
    }
}
