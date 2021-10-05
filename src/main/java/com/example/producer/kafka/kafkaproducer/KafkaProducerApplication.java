package com.example.producer.kafka.kafkaproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication {
    public static void main(String[] args) {
        ConsumerService consumerService  = new ConsumerService();
        SpringApplication app = new SpringApplication(KafkaProducerApplication.class);
        app.run(args);
        consumerService.cosume();

    }

}
