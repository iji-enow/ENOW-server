package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 12..
 */
public class NodeDTO {
    private String roadMapID;
    private String mapID;
    private String topic;
    private String payload;
    private String refer;

    public NodeDTO(String roadMapID, String mapID, String topic, String payload, String refer) {
        this.roadMapID = roadMapID;
        this.mapID = mapID;
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

    public String getRefer() {
        return refer;
    }

    public void setRefer(String refer) {
        this.refer = refer;
    }
}