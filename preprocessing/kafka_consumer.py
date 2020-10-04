# Usage: spark-submit --packages org.apache.spark:spark-streaming-kafka_2.12:3.0.0
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set Kafka config
kafka_broker_hostname = 'localhost'
kafka_broker_portno = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_broker_portno
kafka_topic_input = 'aq-raw-data'

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("Air_quality") \
        .config('packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:2.6.0') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Input from Kafka, Pull data from Kafka topic
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_input) \
        .load()

    # Convert data from Kafka into String type
    df_kafka_string = df_kafka.selectExpr("CAST(value AS STRING) as value")

    # Define schema to read JSON format data
    # Deal with nested structure
    measurement_schema = StructType() \
        .add("location", StringType()) \
        .add("parameter", StringType()) \
        .add("date", MapType(StringType(), DataType())) \
        .add("coordinates", MapType(StringType(), FloatType())) \
        .add("unit", StringType()) \
        .add("country", StringType()) \
        .add("city", StringType()) \
        .add("value", StringType())

    # Parse JSON data
    df_kafka_string_parsed = df_kafka_string.select(
        from_json(df_kafka_string.value, measurement_schema).alias("measurement_data"))

    # Print output to console
    query_console = df_kafka_string_parsed.writeStream.outputMode("complete").format("console").start()
    # Process data until termination signal is received
    query_console.awaitTermination()
