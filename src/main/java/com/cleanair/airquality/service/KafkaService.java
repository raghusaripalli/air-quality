package com.cleanair.airquality.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.logging.Logger;


public class KafkaService {

    private static final Logger logger = Logger.getLogger(String.valueOf(KafkaService.class));

    public static void sendKafkaMessage(String payload, KafkaProducer<String, String> producer, String topic) {
        logger.info("Sending Kafka message: " + payload);
        producer.send(new ProducerRecord<>(topic, payload));
    }
}
