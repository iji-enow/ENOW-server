package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 12..
 */
public class NodeDTO {
    private String roadMapID;
    private String mapID;
    private String topic;
    private String payload;
    private String incomingNode;
    private String outingNode;

    public NodeDTO(String roadMapID, String mapID, String topic, String data, String incomingNode, String outingNode) {
        this.roadMapID = roadMapID;
        this.mapID = mapID;
        this.topic = topic;
        this.payload = payload;
        this.incomingNode = incomingNode;
        this.outingNode = outingNode;
    }

    public String getRoadMapID() {
        return roadMapID;
    }

    public void setRoadMapID(String roadMapID) {
        this.roadMapID = roadMapID;
    }

    public String getMapID() {
        return mapID;
    }

    public void setMapID(String mapID) {
        this.mapID = mapID;
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

    public String getIncomingNode() {
        return incomingNode;
    }

    public void setIncomingNode(String incomingNode) {
        this.incomingNode = incomingNode;
    }

    public String getOutingNode() {
        return outingNode;
    }

    public void setOutingNode(String outingNode) {
        this.outingNode = outingNode;
    }
}