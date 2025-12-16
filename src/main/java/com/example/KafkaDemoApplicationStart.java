package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication
@ComponentScan("com.example")
@EnableWebMvc
@EnableKafka
public class KafkaDemoApplicationStart {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplicationStart.class, args);
	}

}
