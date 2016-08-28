package com.enow.storm.TriggerTopology;

public class TopicStructure {
	private String corporationName;
	private String ServerId;
	private int parseNum;
	private String functionName;
	private String brokerId;
	private String deviceId;
	private String parameter;
	
	//enow/serverid/1/function1/brokerid/deviceid/parameter
	public String getCorporationName() {
		return corporationName;
	}
	public void setCorporationName(String corporationName) {
		this.corporationName = corporationName;
	}
	public String getServerId() {
		return ServerId;
	}
	public void setServerId(String serverId) {
		ServerId = serverId;
	}
	public int getParseNum() {
		return parseNum;
	}
	public void setParseNum(int parseNum) {
		this.parseNum = parseNum;
	}
	public String getFuncionName() {
		return functionName;
	}
	public void setFuncionName(String funcionName) {
		this.functionName = funcionName;
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
	public String getParameter() {
		return parameter;
	}
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	
	public String getAll(){ 
		return "corporationName: " +corporationName + " serverId: " + ServerId + " parseNum: " + parseNum + " functionName:  " + functionName + " brokerId: " + brokerId + " deviceId: " + deviceId + " parameter: " + parameter;
	}
}
