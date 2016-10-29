package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 18..
 */
public class TerminateDTO {
    private String roadMapID;

    public TerminateDTO(String roadMapID) {
        this.roadMapID = roadMapID;
    }

    public String getRoadMapID() {
        return roadMapID;
    }

    public void setRoadMapID(String roadMapID) {
        this.roadMapID = roadMapID;
    }
}
