package com.cleanair.airquality.controller;

import com.cleanair.airquality.dao.Measurement;
import com.cleanair.airquality.service.ConsumerService;
import com.cleanair.airquality.service.ProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping
public class KafkaController {
    private Logger log = LoggerFactory.getLogger(KafkaController.class);
    private final KafkaProducer<String, Measurement> kafkaProducer;
    private final String kafkaTopic;

    @Autowired
    private ProducerService producerService;

    public KafkaController(KafkaProducer<String, Measurement> kafkaProducer, String kafkaTopic, ConsumerService consumerService) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        new Thread(consumerService::consume).start();
    }

    @GetMapping("/produce")
    public ResponseEntity<String> producer() {
        String message = "Request to put data into kafka is being processed successfully";
        try {
            new Thread(() -> producerService.sendKafkaMessage(kafkaProducer, kafkaTopic)).start();
            log.info(message);
            return new ResponseEntity<>(message, HttpStatus.ACCEPTED);
        } catch (Exception ex) {
            log.error("Error while sending message to kafka topic", ex);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
