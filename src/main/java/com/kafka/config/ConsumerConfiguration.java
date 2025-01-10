package com.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.entity.CarLocation;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

@Configuration
@EnableKafka
public class ConsumerConfiguration {

    @Bean
    ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        return objectMapper;
    }
    
   // @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory(){
    	ConcurrentKafkaListenerContainerFactory<String, String> listener = new ConcurrentKafkaListenerContainerFactory<>();
    	listener.setConsumerFactory(this.consumerFactory());
    	return listener;
    }
    // @Bean
    ConsumerFactory<Object, Object> consumerFactory(){
    	Map<String, Object> properties = new HashMap<>();
    	properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    	properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fast-group");
    	properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    	properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    	properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	return new DefaultKafkaConsumerFactory<>(properties);
    }
    
    //@Bean(name="filteringContainer")
    ConcurrentKafkaListenerContainerFactory<Object, Object> ConcurrentKafkaListenerContainerFactory(
    		ConcurrentKafkaListenerContainerFactoryConfigurer configurer, SslBundles sslbundles, ObjectMapper mapper) {
    	
    	ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    	configurer.configure(factory, this.consumerFactory());
    	
    	factory.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>(){
			@Override
			public boolean filter(ConsumerRecord<Object, Object> consumerRecord) {
				try {
					CarLocation carLocation = mapper.readValue(consumerRecord.value().toString(), CarLocation.class);
					return carLocation.getDistance() <= 100;
				}catch(Exception e) {
					return false;
				}
			}
    	});
    	return factory;
    }
}
