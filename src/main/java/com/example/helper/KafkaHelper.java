package com.example.helper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.example.constants.MyConstants;

/**
 * KafkaHelper is a utility class that provides methods to interact with Kafka.
 * It includes methods to get Kafka bootstrap servers, topic, and send messages.
 */

@Component
public class KafkaHelper   {

    @Autowired
    Environment env;

    Properties props = getKafkaProducerProperties();

    public String getKafkaTopic() {
        // Use the topic constant from MyConstants
        return MyConstants.KAFKA_TOPIC;
    }

    // Factory method to create a KafkaProducer. Made protected so tests can spy/override it.
    protected KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(props);
    }

    // Sends a message to the Kafka topic using the provided bootstrap servers.
    public boolean sendMessageToKafka(String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Message cannot be null or empty");
        }
        try (KafkaProducer<String, String> producer = createProducer()) {
            System.out.println("Sending message to Kafka: " + message);
            ProducerRecord<String, String> record = new ProducerRecord<>(getKafkaTopic(), message);
            producer.send(record);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error sending message to Kafka: " + e.getMessage());
        }
        return false;
    }

    // Reads a message from the Kafka topic.
    public String readMessageFromKafka() {
        String message = "Success";
        Properties consumerProps = getKafkaConsumerProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(java.util.Collections.singletonList(getKafkaTopic()));
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                String msg = record.value();
                System.out.println("Read message from Kafka: " + msg);
                //break; // Read only one message
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error reading message from Kafka: " + e.getMessage());
        }
        return message != null ? message : "No message read from Kafka topic: " + getKafkaTopic();
    }

    public String resetOffset(String consumerGroupId) {
        String message = "Success";

        // We want to read from partition 0
        final int TARGET_PARTITION = 0;
        // We want to start reading from offset 10 on that partition
        final long TARGET_OFFSET = 10;

        Properties props = new Properties();
        props.put("bootstrap.servers", MyConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.put("group.id", consumerGroupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Crucial setting: disable auto-commit so our manual seek isn't overwritten
        props.put("enable.auto.commit", "false");
        // Start consuming from the beginning if no offset is found (before seeking)
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 3. Define the TopicPartition
            TopicPartition partitionToSeek = new TopicPartition(MyConstants.KAFKA_TOPIC, TARGET_PARTITION);

            // 4. Manually assign the consumer to the specific partition
            // Note: You must use assign() when you want to use seek().
            // The subscribe() method uses Kafka's group management, which interferes with manual seeks.
            consumer.assign(Collections.singletonList(partitionToSeek));

            // 5. Execute the seek operation!
            System.out.println("Attempting to seek to offset " + TARGET_OFFSET + " on partition " + TARGET_PARTITION + "...");
            consumer.seek(partitionToSeek, TARGET_OFFSET);
            System.out.println("Seek successful. Starting consumption.");

            // 6. Start the polling loop
            int messagesRead = 0;
            while (messagesRead < 5) { // Read 5 messages for demonstration
                // Poll for records with a timeout
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(records != null && !records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                                record.partition(), record.offset(), record.key(), record.value());
                        messagesRead++;
                        if (messagesRead >= 5) break;
                    }
                } else {
                    break;
                }
            }

            // 7. Commit the final offset (if needed)
            // consumer.commitSync();
        } catch (Exception e) {
            e.printStackTrace();
            message = "Failed";
        }
        return message;
    }

    public Properties getKafkaConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", MyConstants.KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put("group.id", MyConstants.KAFKA_CONSUMER_GROUP_ID);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        //consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // offset is set to false to use manual commits. (consumer.commitSync();)
        return consumerProps;
    }


    // Returns the properties required to create a Kafka producer.
    private Properties getKafkaProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", MyConstants.KAFKA_BOOTSTRAP_SERVERS);
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}
