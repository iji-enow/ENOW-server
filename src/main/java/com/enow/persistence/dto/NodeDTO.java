package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 12..
 */
public class NodeDTO {
    private String roadMapID;
    private String nodeID;
    private String topic;
    private String payload;
    private String refer;

    public NodeDTO(String roadMapID, String nodeID, String topic, String payload, String refer) {
        this.roadMapID = roadMapID;
        this.nodeID = nodeID;
        this.topic = topic;
        this.payload = payload;
        this.refer = refer;
    }

    public String getRoadMapID() {
        return roadMapID;
    }

    public void setRoadMapID(String roadMapID) {
        this.roadMapID = roadMapID;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getRefer() {
        return refer;
    }

    public void setRefer(String refer) {
        this.refer = refer;
    }
}