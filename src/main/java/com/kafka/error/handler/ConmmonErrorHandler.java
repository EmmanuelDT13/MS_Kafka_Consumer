package com.kafka.error.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class ConmmonErrorHandler implements CommonErrorHandler{

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {
		System.err.println("I am the common error handler");
		CommonErrorHandler.super.handleOtherException(thrownException, consumer, container, batchListener);
	}

	@Override
	public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
			MessageListenerContainer container) {
		System.err.println("I am the common error handler");
		return CommonErrorHandler.super.handleOne(thrownException, record, consumer, container);
	}

}
