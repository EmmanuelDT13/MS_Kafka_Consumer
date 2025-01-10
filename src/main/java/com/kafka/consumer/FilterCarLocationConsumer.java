package com.kafka.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.CarLocation;

@Service
public class FilterCarLocationConsumer {

	@Autowired
	private ObjectMapper opbjectMapper;
	
	@KafkaListener(topics="t-filtering", groupId="t-filter-all", errorHandler = "carLocationErrorHandler")
	public void listeningWithFilter(String jsonCarLocation) throws JsonMappingException, JsonProcessingException {
		
		CarLocation carLocation = opbjectMapper.readValue(jsonCarLocation, CarLocation.class);
		
		if (carLocation.getDistance() > 95) throw new RuntimeException("The distance is bigger than allowed 95");
		
		System.out.println("Car Location: " + carLocation);
		
	}
	
//	@KafkaListener(topics="t-filtering", groupId="t-filter-far", containerFactory = "filteringContainer")
	public void listeningWithoutFilter(String jsonCarLocation) throws JsonMappingException, JsonProcessingException {
		CarLocation carLocation = opbjectMapper.readValue(jsonCarLocation, CarLocation.class);
		System.out.println("Filter - Car Location: " + carLocation);
	}
	
}
