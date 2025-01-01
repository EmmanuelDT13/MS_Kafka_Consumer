package com.kafka.consumer;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RebalancingConsumer {

	@KafkaListener(topics={"t-alpha", "t-beta"}, groupId="rebalancing-group", concurrency="3")
	private void consumeMessage(String msg) throws InterruptedException {
		TimeUnit.MILLISECONDS.sleep(5);
	}
	
}
