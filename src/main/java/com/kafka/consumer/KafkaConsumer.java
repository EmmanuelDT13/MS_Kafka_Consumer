package com.kafka.consumer;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

//@Service
public class KafkaConsumer {

	@KafkaListener(topics="multiple-partitions", concurrency = "3")
	public void readMessage(String message) throws Exception{
		System.out.println("Este es el mensaje consumido: " + message);
		TimeUnit.SECONDS.sleep(1);
	}	
}
         