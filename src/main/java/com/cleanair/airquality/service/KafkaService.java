package com.cleanair.airquality.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;


public class KafkaService {
    private static final String COMMA = ",";

    public static void sendKafkaMessage(String payload, KafkaProducer<String, String> producer, String topic) {
        producer.send(new ProducerRecord<>(topic, payload));
    }

    public static String consumeMessages(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10L));
        StringBuilder response = new StringBuilder();
        for (ConsumerRecord<String, String> record : records) {
            response.append(record.value());
            response.append(COMMA);
        }
        return response.toString();
    }
}
