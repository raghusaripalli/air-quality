package com.cleanair.airquality.controller;

import com.cleanair.airquality.service.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping
public class KafkaController {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String kafkaTopic;

    public KafkaController(KafkaProducer<String, String> kafkaProducer, String kafkaTopic) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @GetMapping("/produce")
    public ResponseEntity<String> greeting(@RequestParam(value = "word", defaultValue = "abc") String word) {
        KafkaService.sendKafkaMessage(word, kafkaProducer, kafkaTopic);
        return new ResponseEntity<>("Added " + word + " to Kafka Topic", HttpStatus.OK);
    }
}
