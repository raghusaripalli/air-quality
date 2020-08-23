package com.cleanair.airquality.controller;

import com.cleanair.airquality.dao.Measurement;
import com.cleanair.airquality.service.KafkaService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping
public class KafkaController {
    Logger log = LoggerFactory.getLogger(KafkaController.class);
    private final KafkaProducer<String, Measurement> kafkaProducer;
    private final String kafkaTopic;

    @Autowired
    KafkaService kafkaService;

    public KafkaController(KafkaProducer<String, Measurement> kafkaProducer, String kafkaTopic) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @GetMapping("/produce")
    public ResponseEntity<String> producer() {
        try {
            log.info("Received Request to put data into kafka");
            kafkaService.sendKafkaMessage(kafkaProducer, kafkaTopic);
            log.info("Data send to kafka topic successfully.");
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception ex) {
            log.error("Error while sending message to kafka topic", ex);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${zookeeper.groupId}", containerFactory = "customKafkaListenerContainerFactory")
    public void consume(Measurement result) {
        kafkaService.consumeKafkaMessage(result);
    }
}
