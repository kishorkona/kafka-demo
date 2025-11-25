package com.example.backup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTest {


    public static void main(String[] args) {
        try {
            System.out.println("Testing class is working fine.");
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", "localhost:9092");
            consumerProps.put("group.id", "my-kafka-app-group");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("auto.offset.reset", "latest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            String topicName = "javatechie-topic";
            consumer.subscribe(Collections.singletonList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Poll for records with a timeout
                for (ConsumerRecord<String, String> record : records) {
                    //System.out.printf("Consumed message: Topic = %s, Partition = %d, Offset = %d, Key = %s, Value = %s%n",
                    //        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    System.out.println("Consumed message: " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
