package com.kafka.entity;

public class Response {

	private Integer consummingStatus;
	
	private String messageResponse;
	
	public Response() {
		super();
	}

	public Response(Integer consummingStatus, String messageResponse) {
		super();
		this.consummingStatus = consummingStatus;
		this.messageResponse = messageResponse;
	}

	public Integer getConsummingStatus() {
		return consummingStatus;
	}

	public void setConsummingStatus(Integer consummingStatus) {
		this.consummingStatus = consummingStatus;
	}

	public String getMessageResponse() {
		return messageResponse;
	}

	public void setMessageResponse(String messageResponse) {
		this.messageResponse = messageResponse;
	}
	
}
