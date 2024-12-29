package com.kafka.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.Employee;

@Service
public class JsonEmployeeConsumerNotificacions {

	@Autowired
	private ObjectMapper objectMapper;
	
	@KafkaListener(topics="t-employee", groupId = "kafka-employees-notificacion")
	public void consumeMessage(String json) {
		
		try {
			Employee employee = objectMapper.readValue(json, Employee.class);
			System.out.println("NOTIFICACION CONSUMER - Employee got from kafka: " + employee);
		} catch (JsonMappingException e) {
			System.out.println("We couldn't parse the employee from json");
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			System.out.println("We couldn't parse the employee from json");
			e.printStackTrace();
		}
		
	}
	
}
