import sys
import os
import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from utils.logger_config import logger
import requests

class WeatherStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        #os.getenv("KAFKA_BROKER_URL")
        self.kafka_servers = 'localhost:9092'
        
    def create_spark_session(self):
        # .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/weather")
        return SparkSession.builder \
            .appName("LiveMatchesStreamProcessor") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .getOrCreate()
            
    def get_schema(self, topic):
        # os.getenv("SCHEMA_REGISTRY_URL")
        schema_registry = 'http://localhost:8081'
        try:
            response = requests.get(
                f"{schema_registry}/subjects/{topic}-value/versions/latest/schema"
            )
            response.raise_for_status()
            
            logger.info(f"Successfully get '{topic}' schema from Schema Registry")
            
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
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
                
        df_avro = df \
                .select(
                    col("timestamp").alias("kafka_timestamp"),
                    col("offset"),
                    col("partition"),
                    expr("substring(value, 6) as avro_value")
                ) \
                .withColumn("data", from_avro(col("avro_value"), schema)) \
                .select("data.*") \
                .withColumn("event_timestamp", to_timestamp(from_unixtime(col("dt")))) 
                
        return df_avro
    
    def write_stream_to_console(self, df, output_mode, checkpoint_location):
        try:
            logger.info("Start writting stream to console ...")
            query = df.writeStream \
                .outputMode(output_mode) \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", f"./checkpoints/weather/{checkpoint_location}") \
                .start()
                
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            
            query.stop() 
            logger.info("Streaming query stopped successfully.")
    
    
    def threshold_based_anomaly_detect(self, df):
        """
        Detect weather anomalies using threshold
        Args:
            df (DataFrame): Raw dataframe 

        Returns:
            DataFrame: Dataframe with anomaly features
        """
        df = df.withWatermark("event_timestamp", "2 minutes")
        
        anomaly = df \
            .select(
                "event_timestamp",
                "id",
                "city",
                "country",
                "weather_main",
                "weather_description",
                "temperature",
                "pressure",
                "humidity",
                "visibility",
                "wind_spped"
            ) \
            .withColumn("temp_anomaly", expr("CASE WHEN temperature <= 0 OR temperature >= 38 THEN True ELSE False END")) \
            .withColumn("pressure_anomaly", expr("CASE WHEN pressure <= 995 OR pressure >= 1033 THEN True ELSE False END")) \
            .withColumn("visibility_anomaly", expr("CASE WHEN visibility <= 1000 THEN True ELSE False END")) \
            .withColumn("wind_anomaly", expr("CASE WHEN wind_speed >= 10 THEN True ELSE False END")) \
                
        result = anomaly.filter(
            (col("temp_anomaly") == True) |
            (col("pressure_anomaly") == True) |
            (col("visibility_anomaly") == True) |
            (col("wind_anomaly") == True) 
        )
        
        return result
                
    def change_anomaly_detect(self, df):
        """
        Detect weather anomalies on unexpected changes
        Args:
            df (DataFrame): Raw dataframe 

        Returns:
            DataFrame: Dataframe with anomaly features
        """
        df = df.withWatermark("event_timestamp", "2 minutes")
        
        agg_df = df \
                .groupBy(
                "city",
                window("event_timestamp", "30 minutes", "5 minutes").alias("time_window")
            ) \
            .agg(
                avg("temperature").alias("avg_temp"), max("temperature").alias("max_temp"), min("temperature").alias("min_temp"),
                avg("pressure").alias("avg_pressure"), max("pressure").alias("max_pressure"), min("pressure").alias("min_pressure"),
                avg("wind_speed").alias("avg_wind_speed"), max("wind_speed").alias("max_wind"), min("wind_speed").alias("min_wind"), 
                avg("humidity").alias("avg_humidity"), max("humidity").alias("max_humidity"), min("humidity").alias("min_humidity"),
                avg("visibility").alias("avg_vis"), min("visibility").alias("min_vis")
            ) 
            
        analysis = agg_df \
            .withColumn("temp_rise", col("max_temp") - col("avg_temp")) \
            .withColumn("temp_fall", col("avg_temp") - col("min_temp")) \
            .withColumn("pressure_drop", col("max_pressure") - col("min_pressure")) \
            .withColumn("wind_rise", col("max_wind") - col("min_wind")) \
            .withColumn("humidity_rise", col("max_humidity") - col("min_humidity")) 
            
        anomaly = analysis \
            .withColumn("temp_change_anomaly", when(col("temp_rise") >= 3, "rise rapidly")
                                    .when(col("temp_fall") >= 3, "fall rapidly")
                                    .when(col("min_temp") < 0, "below zero")
                                    .otherwise(None)) \
            .withColumn("pressure_change_anomaly", when(col("pressure_drop") >= 2, "drop/unstable") 
                                        .otherwise(None)) \
            .withColumn("wind_change_anomaly", when(col("wind_rise") >= 8, "rise rapidly")
                                    .otherwise(None)) \
            .withColumn("humidity_change_anomaly", when(col("avg_humidity") == 0, None)
                                        .when((col("humidity_rise") / col("avg_humidity")) >= 0.2, "rise rapidly")
                                        .otherwise(None)) \
            .withColumn("visibility_change_anomaly", when((col("avg_vis") > 5000) & (col("min_vis") < 2000), "drop/unstable")
                                            .otherwise(None))
            
        result = anomaly.filter(
            col("temp_change_anomaly").isNotNull() |
            col("pressure_change_anomaly").isNotNull() |
            col("wind_change_anomaly").isNotNull() |
            col("humidity_change_anomaly").isNotNull() |
            col("visibility_change_anomaly").isNotNull()
        )
        
        return result
    
    
    def start_weather_stream_pipeline(self):
        topic = 'weather'
        schema = self.get_schema(topic)
        df_raw = self.read_kafka_stream(topic, schema)
        
        threshold_detect_df = self.threshold_based_anomaly_detect(df_raw)
        self.write_stream_to_console(threshold_detect_df, "append", "threshold_detect")
        
        change_detect_df = self.change_anomaly_detect(df_raw)
        self.write_stream_to_console(change_detect_df, "update", "change_detect")

def main():
    processor = WeatherStreamProcessor()
    processor.start_weather_stream_pipeline()
    
if __name__ == '__main__':
    main()