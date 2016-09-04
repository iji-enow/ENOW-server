package com.enow.dto;

import java.io.Serializable;

/**
 * Created by writtic on 2016. 9. 4..
 */
public class TopicStructure implements Serializable {
	private static final long serialVersionUID = 1L;

	private String corporationName;
	private String serverId;
	private String brokerId;
	private String deviceId;
	private String phaseRoadMapId;
	private String phaseId;
	private String currentMsg;
	private String previousMsg;
	private String currentMapId;
	private String previousMapId;
	

	public TopicStructure() {
		super();
		corporationName = null;
		serverId = null;
		brokerId = null;
		deviceId = null;
		phaseRoadMapId = null;
		phaseId = null;
		currentMsg = null;
		previousMsg = null;
		currentMapId = null;
		previousMapId = null;
		
	}

	public boolean isEmpty() {
		if(corporationName == null && serverId == null && brokerId == null && deviceId == null && phaseRoadMapId == null && phaseId == null && currentMapId == null && previousMapId == null && currentMsg == null  && previousMsg == null){
			return true;
		}
		
		return false;
	}

	public String getCurrentMsg() {
		return currentMsg;
	}

	public void setCurrentMsg(String currentMsg) {
		this.currentMsg = currentMsg;
	}

	public String getPreviousMsg() {
		return previousMsg;
	}

	public void setPreviousMsg(String previousMsg) {
		this.previousMsg = previousMsg;
	}

	public String getCurrentMapId() {
		return currentMapId;
	}

	public void setCurrentMapId(String currentMapId) {
		this.currentMapId = currentMapId;
	}

	public String getPreviousMapId() {
		return previousMapId;
	}

	public void setPreviousMapId(String previousMapId) {
		this.previousMapId = previousMapId;
	}

	public String getCorporationName() {
		return corporationName;
	}

	public void setCorporationName(String corporationName) {
		this.corporationName = corporationName;
	}

	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(String brokerId) {
		this.brokerId = brokerId;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getPhaseRoadMapId() {
		return phaseRoadMapId;
	}

	public void setPhaseRoadMapId(String phaseRoadMapId) {
		this.phaseRoadMapId = phaseRoadMapId;
	}

	public String getPhaseId() {
		return phaseId;
	}

	public void setPhaseId(String phaseId) {
		this.phaseId = phaseId;
	}

	public String output() {
		if(currentMapId == null && previousMapId == null && phaseId == null){
			return corporationName + "/" + serverId + "/" + brokerId + "/" + deviceId + "/" + phaseRoadMapId + "," + currentMsg;
		}else if(previousMapId == null){
			return corporationName + "/" + serverId + "/" + brokerId + "/" + deviceId + "/" + phaseRoadMapId + "/" + phaseId + "/" + currentMapId  + "," + currentMsg;
		}else{
			return corporationName + "/" + serverId + "/" + brokerId + "/" + deviceId + "/" + phaseRoadMapId + "/" + phaseId + "/" + currentMapId + "/" + previousMapId + "|" + currentMsg + "/" + previousMsg;	
		}
	}

	@Override
	public String toString() {
		return "TopicStructure [corporationName=" + corporationName + ", serverId=" + serverId + ", brokerId="
				+ brokerId + ", deviceId=" + deviceId + ", phaseRoadMapId=" + phaseRoadMapId + ", phaseId=" + phaseId
				+ ", currentMsg=" + currentMsg + ", previousMsg=" + previousMsg + ", currentMapId=" + currentMapId
				+ ", previousMapId=" + previousMapId + "]";
	}

}