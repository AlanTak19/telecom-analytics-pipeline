"""
Spark Structured Streaming Job for Telecom Analytics
Processes events from Kafka in real-time with windowed aggregations
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as _sum,
    avg, when, lit, current_timestamp, expr, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TelecomStreamProcessor:
    """Real-time stream processor for telecom events"""
    
    # Event schema (matches generator output)
    EVENT_SCHEMA = StructType([
        StructField("event_id", StringType(), False),
        StructField("msisdn", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_subtype", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("data_mb", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("region", StringType(), True),
        StructField("cell_tower_id", IntegerType(), True),
        StructField("timestamp", StringType(), False)
    ])
    
    def __init__(self, kafka_servers: str = "localhost:9092",
                 kafka_topic: str = "telecom_events",
                 checkpoint_dir: str = "C:/spark-checkpoints",
                 db_url: str = "jdbc:postgresql://localhost:5432/telecom_analytics",
                 db_user: str = "telecom_user",
                 db_password: str = "SecurePassword123!"):
        """Initialize the stream processor"""
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_dir = checkpoint_dir
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.spark = None
        
    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName("TelecomStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
                   "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created")
        return spark
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.EVENT_SCHEMA).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        
        logger.info("✅ Kafka stream configured")
        return parsed_df
    
    def validate_and_filter(self, df):
        """Validate and filter invalid events"""
        logger.info("Applying data validation...")
        
        # Filter invalid data
        valid_df = df.filter(
            # Valid event types
            (col("event_type").isin(['call', 'sms', 'data_session', 
                                     'balance_recharge', 'service_activation'])) &
            # Non-negative durations
            ((col("duration_seconds").isNull()) | (col("duration_seconds") >= 0)) &
            # Non-negative data
            ((col("data_mb").isNull()) | (col("data_mb") >= 0)) &
            # Non-negative amounts
            ((col("amount").isNull()) | (col("amount") >= 0)) &
            # Valid msisdn
            (col("msisdn").isNotNull()) &
            (col("msisdn") != "")
        )
        
        logger.info("✅ Validation filters applied")
        return valid_df
    
    def detect_anomalies(self, df):
        """Detect anomalous events"""
        # Anomaly detection rules
        anomalies = df.filter(
            # Very long calls (> 4 hours)
            ((col("event_type") == "call") & (col("duration_seconds") > 14400)) |
            # Extremely high data usage (> 5 GB in one session)
            ((col("event_type") == "data_session") & (col("data_mb") > 5000)) |
            # Very large recharges (> 50000 tenge)
            ((col("event_type") == "balance_recharge") & (col("amount") > 50000))
        )
        
        # Add anomaly reason
        anomalies = anomalies.withColumn(
            "anomaly_type",
            when(col("duration_seconds") > 14400, "excessive_call_duration")
            .when(col("data_mb") > 5000, "excessive_data_usage")
            .when(col("amount") > 50000, "excessive_recharge")
            .otherwise("unknown")
        )
        
        anomalies = anomalies.withColumn(
            "anomaly_reason",
            when(col("duration_seconds") > 14400, 
                 expr("concat('Call duration: ', duration_seconds, ' seconds')"))
            .when(col("data_mb") > 5000,
                 expr("concat('Data usage: ', data_mb, ' MB')"))
            .when(col("amount") > 50000,
                 expr("concat('Recharge amount: ', amount, ' tenge')"))
            .otherwise("Unknown anomaly")
        )
        
        anomalies = anomalies.withColumn("severity", lit("high"))
        anomalies = anomalies.withColumn("detected_at", current_timestamp())
        
        return anomalies
    
    def aggregate_5min_window(self, df):
        """Aggregate metrics in 5-minute windows"""
        logger.info("Creating 5-minute window aggregations...")
        
        # Event type metrics (5 min window)
        event_metrics = df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("event_type"),
                col("event_subtype")
            ) \
            .agg(
                count("*").alias("count_events"),
                _sum(coalesce(col("duration_seconds"), lit(0))).alias("total_duration_seconds"),
                _sum(coalesce(col("data_mb"), lit(0))).alias("total_data_mb"),
                _sum(coalesce(col("amount"), lit(0))).alias("total_amount"),
                avg(col("duration_seconds")).alias("avg_duration_seconds"),
                avg(col("data_mb")).alias("avg_data_mb")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("event_type_5min").alias("metric_type"),
                lit(None).cast("string").alias("region"),
                col("event_type"),
                col("event_subtype"),
                col("count_events"),
                col("total_duration_seconds"),
                (col("total_data_mb") / 1024.0).alias("total_data_gb"),
                col("total_amount"),
                col("avg_duration_seconds"),
                col("avg_data_mb"),
                current_timestamp().alias("created_at")
            )
        
        return event_metrics
    
    def aggregate_10min_window(self, df):
        """Aggregate metrics in 10-minute windows by region"""
        logger.info("Creating 10-minute window aggregations...")
        
        # Regional metrics (10 min window)
        regional_metrics = df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("region"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("count_events"),
                _sum(coalesce(col("duration_seconds"), lit(0))).alias("total_duration_seconds"),
                _sum(coalesce(col("data_mb"), lit(0))).alias("total_data_mb"),
                _sum(coalesce(col("amount"), lit(0))).alias("total_amount")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("regional_10min").alias("metric_type"),
                col("region"),
                col("event_type"),
                lit(None).cast("string").alias("event_subtype"),
                col("count_events"),
                col("total_duration_seconds"),
                (col("total_data_mb") / 1024.0).alias("total_data_gb"),
                col("total_amount"),
                lit(None).cast("double").alias("avg_duration_seconds"),
                lit(None).cast("double").alias("avg_data_mb"),
                current_timestamp().alias("created_at")
            )
        
        return regional_metrics
    
    def write_to_postgres(self, df, table_name: str, mode: str = "append"):
        """Write DataFrame to PostgreSQL"""
        
        def write_batch(batch_df, batch_id):
            """Write each micro-batch to PostgreSQL"""
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.db_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_user) \
                    .option("password", self.db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode(mode) \
                    .save()
                
                count = batch_df.count()
                logger.info(f"✅ Written {count} records to {table_name} (batch {batch_id})")
                
            except Exception as e:
                logger.error(f"❌ Failed to write batch {batch_id} to {table_name}: {e}")
        
        return write_batch
    
    def run(self):
        """Run the streaming pipeline"""
        try:
            logger.info("🚀 Starting Telecom Stream Processor...")
            
            # Create Spark session
            self.spark = self.create_spark_session()
            
            # Read from Kafka
            raw_stream = self.read_from_kafka()
            
            # Validate and filter
            valid_stream = self.validate_and_filter(raw_stream)
            
            # Detect anomalies
            anomalies = self.detect_anomalies(valid_stream)
            
            # Write anomalies to PostgreSQL
            anomaly_query = anomalies.select(
                col("event_id"),
                col("msisdn"),
                col("anomaly_type"),
                col("anomaly_reason"),
                col("severity"),
                col("detected_at")
            ).writeStream \
                .foreachBatch(self.write_to_postgres(anomalies, "anomalies")) \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/anomalies") \
                .start()
            
            logger.info("✅ Anomaly detection stream started")
            
            # 5-minute window aggregations
            metrics_5min = self.aggregate_5min_window(valid_stream)
            
            metrics_5min_query = metrics_5min.writeStream \
                .foreachBatch(self.write_to_postgres(metrics_5min, "real_time_metrics")) \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/metrics_5min") \
                .start()
            
            logger.info("✅ 5-minute metrics stream started")
            
            # 10-minute window aggregations
            metrics_10min = self.aggregate_10min_window(valid_stream)
            
            metrics_10min_query = metrics_10min.writeStream \
                .foreachBatch(self.write_to_postgres(metrics_10min, "real_time_metrics")) \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/metrics_10min") \
                .start()
            
            logger.info("✅ 10-minute metrics stream started")
            
            # Also write raw events to PostgreSQL
            raw_events_query = valid_stream.writeStream \
                .foreachBatch(self.write_to_postgres(valid_stream, "raw_events")) \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/raw_events") \
                .start()
            
            logger.info("✅ Raw events stream started")
            
            logger.info("📊 All streams running. Press Ctrl+C to stop.")
            logger.info(f"📁 Checkpoint directory: {self.checkpoint_dir}")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
            
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stopping streams...")
        except Exception as e:
            logger.error(f"❌ Stream processing error: {e}", exc_info=True)
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Telecom Stream Processor')
    parser.add_argument('--kafka', default='localhost:9092', 
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='telecom_events',
                       help='Kafka topic to consume from')
    parser.add_argument('--checkpoint', default='C:/spark-checkpoints',
                       help='Checkpoint directory')
    parser.add_argument('--db-url', 
                       default='jdbc:postgresql://localhost:5432/telecom_analytics',
                       help='PostgreSQL JDBC URL')
    parser.add_argument('--db-user', default='telecom_user',
                       help='Database user')
    parser.add_argument('--db-password', default='SecurePassword123!',
                       help='Database password')
    
    args = parser.parse_args()
    
    # Create and run processor
    processor = TelecomStreamProcessor(
        kafka_servers=args.kafka,
        kafka_topic=args.topic,
        checkpoint_dir=args.checkpoint,
        db_url=args.db_url,
        db_user=args.db_user,
        db_password=args.db_password
    )
    
    processor.run()


if __name__ == '__main__':
    main()