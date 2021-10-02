package com.example.producer.kafka.kafkaproducer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class KafkaProducerApplication {

    private final static Logger logger = LoggerFactory.getLogger(KafkaProducerApplication.class);

    private final static String TOPIC_NAME = "test-waldo";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "easywaldo-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        //SpringApplication.run(KafkaProducerApplication.class, args);
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("recode value is {}", record.value());
            }
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    logger.error("Commit failed");
                    logger.error("Commit failed for offsets {}", offsets, exception);
                }
                else {
                    logger.info("Commit succeed");
                }
            });
        }
    }

}
