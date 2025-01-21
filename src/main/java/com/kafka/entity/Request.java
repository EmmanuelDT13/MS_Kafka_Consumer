package com.kafka.entity;

import java.util.UUID;

public class Request {

	private UUID requestId;
	
	private Long productId;
	
	private Long clientId;

	public Request() {
	}
	
	public Request(UUID requestId, Long productId, Long clientId) {
		super();
		this.requestId = requestId;
		this.productId = productId;
		this.clientId = clientId;
	}

	public UUID getRequestId() {
		return requestId;
	}

	public void setRequestId(UUID requestId) {
		this.requestId = requestId;
	}

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public Long getClientId() {
		return clientId;
	}

	public void setClientId(Long clientId) {
		this.clientId = clientId;
	}
	
}
