package com.example.listener;

import com.example.constants.MyConstants;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MyKafkaListener {

    @KafkaListener(id = "myListener", topics = MyConstants.KAFKA_TOPIC, groupId = MyConstants.KAFKA_CONSUMER_LISTENER_GROUP_ID)
    public void listen(String message) {
        System.out.println("Received message: " + message);
    }
}
