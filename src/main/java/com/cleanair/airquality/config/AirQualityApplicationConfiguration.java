package com.cleanair.airquality.config;

import com.cleanair.airquality.controller.KafkaController;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Configuration
public class AirQualityApplicationConfiguration {

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Bean
    public Properties producerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return producerProperties;
    }

    @Bean
    public Properties consumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("group.id", zookeeperGroupId);
        consumerProperties.put("zookeeper.session.timeout.ms", "6000");
        consumerProperties.put("zookeeper.sync.time.ms", "2000");
        consumerProperties.put("auto.commit.enable", "false");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("consumer.timeout.ms", "-1");
        consumerProperties.put("max.poll.records", "5");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerProperties;
    }

    @Bean
    @DependsOn({"producerProperties"})
    public KafkaProducer<String, String> kafkaProducer(Properties producerProperties) {
        return new KafkaProducer<>(producerProperties);
    }

    @Bean
    @DependsOn({"consumerProperties"})
    public KafkaConsumer<String, String> kafkaConsumer(Properties consumerProperties) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));
        return kafkaConsumer;
    }

    @Bean
    @DependsOn({"producerProperties"})
    public AdminClient adminClient(Properties producerProperties) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(producerProperties);
        ListTopicsResult topics = adminClient.listTopics();
        Set<String> topicNames = topics.names().get();
        if (!topicNames.contains(kafkaTopic)) {
            NewTopic newTopic = new NewTopic(kafkaTopic, 1, (short) 1);
            List<NewTopic> newTopics = new ArrayList<>();
            newTopics.add(newTopic);
            adminClient.createTopics(newTopics);
        }
        return adminClient;
    }

    @Bean
    @DependsOn({"kafkaProducer", "kafkaConsumer"})
    public KafkaController kafkaController(KafkaProducer<String, String> kafkaProducer, KafkaConsumer<String, String> kafkaConsumer) {
        return new KafkaController(kafkaProducer, kafkaConsumer, kafkaTopic);
    }
}
