package com.enow.dto;

public class TopicStructure {
	private String corporationName;
	private String serverId;
	private String brokerId;
	private String deviceId;
	private String phaseRoadMapId;
	private String msg;
	private String mapId;
	
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public String getMapId() {
		return mapId;
	}
	public void setMapId(String mapId) {
		this.mapId = mapId;
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
	public String showAll(){
		return "corporationName : "+ corporationName+" serverId : " + serverId + " brokerId : " + brokerId + " deviceId : " + deviceId + " PhaseRoadMapId : " + phaseRoadMapId;
	}
	
	public String output(){
		return corporationName +"/"+ serverId +"/"+ brokerId +"/"+ deviceId +"/"+ phaseRoadMapId +"/"+ mapId +" "+msg;
	}
	
}
