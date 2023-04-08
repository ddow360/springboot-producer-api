package com.dow.design.springboot.producer.api.controller;

import com.dow.design.springboot.producer.api.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dow.design.springboot.producer.api.util.Constants.PRODUCER_TOPIC;

@RestController
@Slf4j
@Import(KafkaConfig.class)
public class KafkaProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    AtomicInteger atomicInteger = new AtomicInteger();

    @PostMapping("/produce")
    public ResponseEntity<String> produceToKafka(@RequestBody String data) {

        log.info("Request received! Message to be sent to Kafka: {}", data);

        // Creating a Producer Record with a traceId added to the header an incremental key to topic PRODUCER_TOPIC
        ProducerRecord<String, String> record = new ProducerRecord<>(PRODUCER_TOPIC, String.valueOf(atomicInteger.getAndIncrement()) , data);

        Headers headers = record.headers();
        headers.add("traceId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        // Sending an Async string serialized record to Kafka
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        try {
            // Wait for the future to complete
            future.get();

            // Return success response with HTTP status code 200
            return ResponseEntity.ok(String.format("Produced to topic %s successfully on partition %s, offset %s!",PRODUCER_TOPIC, future.get().getRecordMetadata().partition(),
                    future.get().getRecordMetadata().offset()));

        } catch (InterruptedException | ExecutionException ex) {
            // handle exception and return error response with HTTP status code 500
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                    String.format("Failed to produce to topic %s.. error has occurred %s!", PRODUCER_TOPIC, ex.getMessage()));
        }
    }
}
