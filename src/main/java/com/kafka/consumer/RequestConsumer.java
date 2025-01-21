package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.Request;
import com.kafka.entity.Response;
import com.kafka.exception.RequestNotValidException;

@Service
public class RequestConsumer {

	@Autowired
	private ObjectMapper objectMapper;
	

	@KafkaListener(topics="request-topic", containerFactory = "request-topic-container", concurrency="3")	
	@SendTo(value = "responses-topic")	//@SendTo allows us to send an answer to one topic once the process has finished.
//	@RetryableTopic(	//I have commented this annotation because in the configuration bean I have created configuration like this. But here you can see how to configure it by annotation.
//			autoCreateTopics="false", //Allows spring to create in kafka the necessary topics to manage the dead letter topic. If false, you must create each topic before to start the application.
//			attempts="3", //Represents the number of retries that each error message will be tried to process before going to dead letter topic.
//			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE, //Every retry to process the message will have the retry id as prefix.
//			backoff = @Backoff(
//					delay=3000,	//The base delay for each retry.
//					maxDelay=10000,	//The maximum delay that each retry must have.
//					multiplier=1.5,	//How much the base delay is going to be multiplied for growing for each retry.
//					random=true	//Allows spring to manage the delay for each retry depending of the circumstances.
//					),
//			dltTopicSuffix="-dead" //Allows us to modify the default suffix for each dead letter topic. (The default suffix is .dlt) Example: my-topic.dlt -> my-topic-dead
//			)
	public Response requestConsuming(/*Consumer record is a wrapper object, like ResponseEntity. But we may receive only the object as well.*/ ConsumerRecord<String, String> request) throws JsonMappingException, JsonProcessingException {
		
			Request request2 = objectMapper.readValue(request.value(), Request.class); //Here we convert the json string message to Object.

			if (request2.getProductId() == 12L) {
				throw new RequestNotValidException("El producto no puede ser 1");
			} 
			
			System.out.println(request.value());
			return new Response(200, "Message readed successfully.");
	
	}
	
}
