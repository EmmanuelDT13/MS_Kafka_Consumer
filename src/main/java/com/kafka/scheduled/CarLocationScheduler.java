package com.kafka.scheduled;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class CarLocationScheduler {

	@Autowired
	private KafkaListenerEndpointRegistry registry;
	
	@Scheduled(cron = "0 29 21 * * *")
	private void stop() {
		System.out.println("Stopping the consumer");
		registry.getListenerContainer("hello-consumer").pause();
	}
	
	@Scheduled(cron = "0 30 21 * * *")
	private void resume() {
		System.out.println("Resuming the consumer");
		registry.getListenerContainer("hello-consumer").resume();
	}
	
}
