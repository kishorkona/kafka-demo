package com.example.controller;

import com.example.helper.KafkaHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
public class KafkaMessageRestControllerTest {

    private MockMvc mockMvc;

    @Mock
    private KafkaHelper kafkaHelper;

    @InjectMocks
    private KafkaMessageRestController controller;

    @BeforeEach
    void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void testSendMessage_success() throws Exception {
        String message = "test-message";
        when(kafkaHelper.sendMessageToKafka(message)).thenReturn(true);

        mockMvc.perform(post("/kafka/sendMessage")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content(message))
                .andExpect(status().isOk())
                .andExpect(content().string("Message sent successfully"));
    }

    @Test
    void testSendMessage_failure() throws Exception {
        String message = "test-message";
        when(kafkaHelper.sendMessageToKafka(message)).thenReturn(false);

        mockMvc.perform(post("/kafka/sendMessage")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content(message))
                .andExpect(status().isInternalServerError())
                .andExpect(content().string("Failed to send message"));
    }
}
