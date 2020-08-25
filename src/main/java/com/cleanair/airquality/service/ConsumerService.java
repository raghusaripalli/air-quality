package com.cleanair.airquality.service;

import com.cleanair.airquality.dao.Measurement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class ConsumerService {
    private Logger log = LoggerFactory.getLogger(ConsumerService.class);
    private SparkSession sparkSession;
    private String kafkaBootstrapServers;
    private String kafkaTopic;

    public ConsumerService(SparkSession sparkSession, String kafkaBootstrapServers, String kafkaTopic) {
        this.sparkSession = sparkSession;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        log.info("Consumer Initialized");
    }

    public void consume() {
        try {
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("location", DataTypes.StringType, false),
                    DataTypes.createStructField("parameter", DataTypes.StringType, false),
                    DataTypes.createStructField("date", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false),
                    DataTypes.createStructField("coordinates", DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType), false),
                    DataTypes.createStructField("unit", DataTypes.StringType, false),
                    DataTypes.createStructField("country", DataTypes.StringType, false),
                    DataTypes.createStructField("city", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false)});

            Dataset<Measurement> ds = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", kafkaTopic)
                    .load()
                    .selectExpr("CAST(value AS STRING) as measurement")
                    .select(functions.from_json(functions.col("measurement"), schema).as("json"))
                    .select("json.*")
                    .as(Encoders.bean(Measurement.class));

            StreamingQuery query = ds.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();
            log.info(query.status().message());
            query.awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
