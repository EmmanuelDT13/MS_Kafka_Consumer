package com.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class HandleErrorConsumer {

	@KafkaListener(topics="my-topic", groupId="vip-group")
	public void consumeWithHandleError() {
		
	}
	
}
