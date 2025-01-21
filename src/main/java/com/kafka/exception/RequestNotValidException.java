package com.kafka.exception;

public class RequestNotValidException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	public RequestNotValidException(String msg) {
		super("Something went wrong: " + msg);
	}
	
	
}
