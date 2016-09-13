package com.enow.persistence.dto;

/**
 * Created by writtic on 2016. 9. 13..
 */
public class StatusDTO {
    private String topic;
    private String payload;

    public StatusDTO(String topic, String payload) {
        this.topic = topic;
        this.payload = payload;
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
}
