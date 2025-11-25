package com.example.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.example.helper.KafkaHelper;
import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
@RequestMapping("/kafka")
public class KafkaMessageRestController {
    // This class is currently empty, but it can be used to handle REST requests related to Kafka messages.
    // You can add methods to send messages, receive messages, or any other Kafka-related operations here.

    @Autowired
    public KafkaHelper kafkaHelper;

    // Example method to send a message (to be implemented):
    @PostMapping("/sendMessage")
    public ResponseEntity<String> sendMessage(@RequestBody String message) {
         boolean result = kafkaHelper.sendMessageToKafka(message);
         return result ? ResponseEntity.ok("Message sent successfully") : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to send message");
    }

    // Example method to test the controller (to be implemented):
    @GetMapping("/readMessage")
    public ResponseEntity<String> readMessage() {
        String message = kafkaHelper.readMessageFromKafka();
        if (message != null) {
            return ResponseEntity.ok(message);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No message found");
        }
    }

    @PostMapping("/resetOffset")
    public ResponseEntity<String> resetOffset(@RequestBody String consumerGroupId) {
        String message = kafkaHelper.resetOffset(consumerGroupId);
        if (message != null) {
            return ResponseEntity.ok(message);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No message found");
        }
    }

    @GetMapping("/test")
    public String testMessage() {
        return "success";
    }

}
