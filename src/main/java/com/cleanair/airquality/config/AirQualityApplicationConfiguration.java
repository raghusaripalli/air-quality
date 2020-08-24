package com.cleanair.airquality.config;

import com.cleanair.airquality.controller.KafkaController;
import com.cleanair.airquality.dao.Measurement;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Configuration
public class AirQualityApplicationConfiguration {

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${kafka.producer.keySerializer}")
    private String keySerializer;

    @Value("${kafka.consumer.keyDeserializer}")
    private String keyDeserializer;

    @Value("${kafka.producer.valueSerializer}")
    private String valueSerializer;

    @Value("${kafka.consumer.valueDeserializer}")
    private String valueDeserializer;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.appName}")
    private String appName;

    @Bean
    public Properties producerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", keySerializer);
        producerProperties.put("value.serializer", valueSerializer);
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
        consumerProperties.put("spring.json.trusted.packages", "*");
        consumerProperties.put("value.deserializer", valueDeserializer);
        consumerProperties.put("key.deserializer", keyDeserializer);
        return consumerProperties;
    }

    @Bean
    @DependsOn({"consumerProperties"})
    public ConcurrentKafkaListenerContainerFactory<String, Measurement> customKafkaListenerContainerFactory(Properties consumerProperties) {
        ConcurrentKafkaListenerContainerFactory<String, Measurement> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory(consumerProperties));
        return factory;
    }

    @Bean
    @DependsOn({"producerProperties"})
    public KafkaProducer<String, Measurement> kafkaProducer(Properties producerProperties) {
        return new KafkaProducer<>(producerProperties);
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
    @DependsOn({"kafkaProducer"})
    public KafkaController kafkaController(KafkaProducer<String, Measurement> kafkaProducer) {
        return new KafkaController(kafkaProducer, kafkaTopic);
    }

    @Bean
    @DependsOn({"kafkaProducer"})
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.jars.repositories", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
                .getOrCreate();
    }

    @Bean
    @DependsOn({"kafkaProducer", "sparkSession"})
    public Dataset<Row> measurementDataSet(SparkSession sparkSession) {
        Dataset<Row> ds = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .load();
        ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        return ds;
    }
}
