package com.example.helper;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaHelperTest {

    @Test
    void testSendMessageToKafka_success() throws Exception {
        KafkaProducer<String, String> mockProducer = Mockito.mock(org.apache.kafka.clients.producer.KafkaProducer.class);
        // stub send to return a typed Future<RecordMetadata>
        CompletableFuture<RecordMetadata> mockFuture = CompletableFuture.completedFuture(null);
        when(mockProducer.send(Mockito.any())).thenReturn(mockFuture);

        KafkaHelper helper = Mockito.spy(new KafkaHelper());
        // when helper.createProducer() is called, return our mockProducer
        doReturn(mockProducer).when(helper).createProducer();

        boolean result = helper.sendMessageToKafka("test-message");
        assertTrue(result);
    }

    @Test
    void testSendMessageToKafka_nullOrEmpty() {
        KafkaHelper helper = new KafkaHelper();
        assertThrows(IllegalArgumentException.class, () -> helper.sendMessageToKafka(""));
        assertThrows(IllegalArgumentException.class, () -> helper.sendMessageToKafka(null));
    }
}
