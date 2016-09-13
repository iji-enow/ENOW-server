package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 12..
 */
public class PeerDTO {
    private String roadMapID;
    private String mapID;
    private String state;
    private String payload;

    public PeerDTO(String roadMapID, String mapID, String payload) {
        this.roadMapID = roadMapID;
        this.mapID = mapID;
        this.payload = payload;
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

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}