package com.kafka.error.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service(value = "carLocationErrorHandler")
public class ConsumerErrorHandler implements ConsumerAwareListenerErrorHandler{

	@Override
	public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
		
		
		System.out.println("One consumer type: " + consumer.getClass() + " has failed.");
		System.out.println("Message: "+ message);
		System.out.println("Exception: "+ exception);

		throw exception;
		
	}

}
