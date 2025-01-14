package com.kafka.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.CarLocation;

@Service
public class FilterCarLocationConsumer {

	@Autowired
	private ObjectMapper opbjectMapper;
	
	@RetryableTopic(
			autoCreateTopics="true", //Allows spring to create in kafka the necesary topics to manage the dead letter topic. If false, you must create each topic before to start the application.
			attempts="3", //Represents the number of retries that each error message will be tried to process before going to dead letter topic.
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE, //Every retry to process the message will have the retry id as prefix.
			backoff = @Backoff(
					delay=3000,	//The base delay for each retry.
					maxDelay=10000,	//The maximum delay that each retry must have.
					multiplier=1.5,	//How much the base delay is going to be multiplied for growing for each retry.
					random=true	//Allows spring to manage the delay for each retry depending of the circumstances.
					),
			dltTopicSuffix="-dead" //Allows us to modify the default suffix for each dead letter topic. (The default suffix is .dlt) Example: my-topic.dlt -> my-topic-dead
			)
	@KafkaListener(topics="t-filtering", groupId="t-filter-all", errorHandler = "carLocationErrorHandler", containerFactory = "dead-letter", id="hello-consumer")
	public void listeningWithFilter(String jsonCarLocation) throws JsonMappingException, JsonProcessingException {
		
		CarLocation carLocation = opbjectMapper.readValue(jsonCarLocation, CarLocation.class);
		
		//if (carLocation.getDistance() > 95) throw new RuntimeException("The distance is bigger than allowed 95");
		
		System.out.println("Car Location: " + carLocation);
		
	}
	
//	@KafkaListener(topics="t-filtering", groupId="t-filter-far", containerFactory = "filteringContainer")
	public void listeningWithoutFilter(String jsonCarLocation) throws JsonMappingException, JsonProcessingException {
		CarLocation carLocation = opbjectMapper.readValue(jsonCarLocation, CarLocation.class);
		System.out.println("Filter - Car Location: " + carLocation);
	}
	
}
