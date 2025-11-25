package com.example.config;

import com.example.constants.MyConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    //https://www.baeldung.com/kafka-spring-boot-dynamically-manage-listeners
    //https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerConfigs());
        factory.setConcurrency(3); // Example: set concurrency
        factory.getContainerProperties().setPollTimeout(3000); // Example: set poll timeout
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", MyConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.put("group.id", MyConstants.KAFKA_CONSUMER_GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
