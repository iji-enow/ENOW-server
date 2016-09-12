package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 12..
 */
public class PeerDTO {
    private int peerID;
    private String mapID;
    private String state;
    private String payload;

    public PeerDTO(int peerID, String mapID, String state, String payload) {
        this.peerID = peerID;
        this.mapID = mapID;
        this.state = state;
        this.payload = payload;
    }

    public int getPeerID() {
        return peerID;
    }

    public String getMapID() {
        return mapID;
    }

    public void setMapID(String mapID) {
        this.mapID = mapID;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPayload() { return payload; }

    public void setPayload(String payload) { this.payload = payload; }
}