package com.example.producer.kafka.kafkaproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

@Service
public class ConsumerService {
    private final static Logger logger = LoggerFactory.getLogger(KafkaProducerApplication.class);

    private final String TOPIC_NAME = "test-waldo";
    private final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final String GROUP_ID = "easywaldo-group";

    public void cosume() {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        long commitOffset = 0;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record value is {}", record.value());
                commitOffset = record.offset();
            }
            long finalCommitOffset = commitOffset;
            //consumer.endOffsets(0, Duration.ofSeconds(1)).entrySet().
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    logger.error("Commit failed");
                    logger.error("Commit failed for offsets {}", offsets, exception);
                }
                else {
                    //int topPartition = offsets.keySet().stream().findFirst().get().partition();
                    logger.info("commit offset : " + finalCommitOffset);
                    logger.info("current offset : " + offsets.values().stream().max(Comparator.comparing(OffsetAndMetadata::offset)).get().offset());
                    if (finalCommitOffset != offsets.values().stream().max(Comparator.comparing(OffsetAndMetadata::offset)).get().offset()) {
                        logger.info("Commit succeed");
                    }
                }
            });
        }
    }


}
