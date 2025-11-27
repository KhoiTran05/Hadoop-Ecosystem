from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
from utils.logger_config import logger
import os
import requests

class WeatherStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.kafka_servers = os.getenv("KAFKA_BROKER_URL")
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("LiveMatchesStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/weather") \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
            .getOrCreate()
            
    def get_schema(self, topic):
        schema_registry = os.getenv("SCHEMA_REGISTRY_URL")
        try:
            response = requests.get(
                f"{schema_registry}/subjects/{topic}-value/versions/latest/schema"
            )
            response.raise_for_status()
            
            logger.info(f"Successfully get {topic} schema from Schema Registry")
            
            avro_schema = response.text
            return avro_schema
        except Exception as e:
            logger.error(f"Failed to get schema for {topic}: {str(e)}")
            return None
        
    def read_kafka_stream(self, topic, schema):
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
                
        df_avro = df \
                .select(
                    col("timestamp").alias("kafka_timestamp"),
                    col("offset"),
                    col("partition"),
                    expr("substring(value, 6) as avro_value")
                ) \
                .select(from_avro(col("avro_value"), schema).alias("data")) \
                .select("data.*")
                
        return df_avro
                
    def write_stream_to_console(self, df):
        query = df.writeStream \
            .format("console") \
            .option("truncate", False) \
            .start()
            
        query.awaitTermination()
        
    def start_weather_stream_pipeline(self):
        topic = 'weather'
        schema = self.get_schema(topic)
        df_raw = self.read_kafka_stream(topic, schema)
        self.write_stream_to_console(df_raw)
        

def main():
    processor = WeatherStreamProcessor()
    processor.start_weather_stream_pipeline()