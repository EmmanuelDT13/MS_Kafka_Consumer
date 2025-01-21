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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.entity.Request;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@EnableScheduling
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
    ConsumerFactory<Object, Object> consumerFactory(){
    	Map<String, Object> properties = new HashMap<>();
    	properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    	properties.put(ConsumerConfig.GROUP_ID_CONFIG, "request-group");
    	properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    	properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    	properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	return new DefaultKafkaConsumerFactory<>(properties);
    }
    
    @Bean(name="request-topic-container")
    ConcurrentKafkaListenerContainerFactory<Object, Object> createRequestContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, SslBundles bundles, KafkaTemplate<Object,Object> kafkaTemplate){
    	
    	//Container logic creation logic
    	ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    	configurer.configure(factory, this.consumerFactory());

    	//Filter logic
    	factory.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>(){
			@Override
			public boolean filter(ConsumerRecord<Object, Object> consumerRecord) {
				
				Request request;
				try {
					request = objectMapper().readValue(consumerRecord.value().toString(), Request.class);
					return request.getClientId() == 0;
				} catch (JsonProcessingException e) {
					e.printStackTrace();
					return false;
				}
			}
    	});
    	
    	//Dead letter topic and retry login
    	DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> new TopicPartition("request-dead-lettertopic", record.partition()));
    	factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(10000,3)));
    	return factory;
    }
}
