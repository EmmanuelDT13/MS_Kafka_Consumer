package com.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CounterConsumers {

	@KafkaListener(topics="counter-topic", groupId="fast-group", containerFactory = "concurrentKafkaListenerContainerFactory")
	public void fastConsumer(String msg) {
		System.out.println("FAST GROUP: " + msg);
	}
	
	@KafkaListener(topics="counter-topic", groupId="slow-group")
	public void slowConsumer(String msg) throws InterruptedException {
		Thread.sleep(5000);
		System.out.println("SLOW GROUP: " + msg);
		
	}
	
}
