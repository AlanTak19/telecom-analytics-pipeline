"""
Docker-Compatible Spark Structured Streaming Job
Runs inside Docker container with proper networking
"""

import sys
import logging
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
    
    # Event schema
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
    
    def __init__(self):
        """Initialize with Docker networking"""
        self.kafka_servers = "kafka:29092"  # Internal Docker network
        self.kafka_topic = "telecom_events"
        self.checkpoint_dir = "/tmp/spark-checkpoint"
        self.db_url = "jdbc:postgresql://postgres:5432/telecom_analytics"
        self.db_user = "telecom_user"
        self.db_password = "SecurePassword123!"
        self.spark = None
        
    def create_spark_session(self) -> SparkSession:
        """Create Spark session"""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName("TelecomStreamProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created")
        return spark
    
    def read_from_kafka(self):
        """Read from Kafka"""
        logger.info(f"Reading from Kafka: {self.kafka_servers}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.EVENT_SCHEMA).alias("data")
        ).select("data.*")
        
        # Convert timestamp
        parsed_df = parsed_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        
        logger.info("✅ Kafka stream configured")
        return parsed_df
    
    def validate_and_filter(self, df):
        """Validate events"""
        valid_df = df.filter(
            (col("event_type").isin(['call', 'sms', 'data_session', 
                                     'balance_recharge', 'service_activation'])) &
            ((col("duration_seconds").isNull()) | (col("duration_seconds") >= 0)) &
            ((col("data_mb").isNull()) | (col("data_mb") >= 0)) &
            ((col("amount").isNull()) | (col("amount") >= 0)) &
            (col("msisdn").isNotNull()) &
            (col("msisdn") != "")
        )
        return valid_df
    
    def detect_anomalies(self, df):
        """Detect anomalies"""
        anomalies = df.filter(
            ((col("event_type") == "call") & (col("duration_seconds") > 14400)) |
            ((col("event_type") == "data_session") & (col("data_mb") > 5000)) |
            ((col("event_type") == "balance_recharge") & (col("amount") > 50000))
        )
        
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
            .otherwise("Unknown")
        )
        
        anomalies = anomalies.withColumn("severity", lit("high"))
        anomalies = anomalies.withColumn("detected_at", current_timestamp())
        
        return anomalies
    
    def aggregate_5min_window(self, df):
        """5-minute aggregations"""
        metrics = df \
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
        
        return metrics
    
    def aggregate_10min_window(self, df):
        """10-minute regional aggregations"""
        metrics = df \
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
        
        return metrics
    
    
    def write_to_postgres(self, df, table_name: str):
        """Write to PostgreSQL with proper type casting"""
        
        def write_batch(batch_df, batch_id):
            try:
                # Cast event_id to string for raw_events
                if table_name == "raw_events":
                    batch_df = batch_df.withColumn("event_id", col("event_id").cast("string"))
                
                # Cast event_id to string for anomalies 
                if table_name == "anomalies":
                    batch_df = batch_df.withColumn("event_id", col("event_id").cast("string"))
                    
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.db_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_user) \
                    .option("password", self.db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                    
                count = batch_df.count()
                logger.info(f"✅ Wrote {count} records to {table_name} (batch {batch_id})")
                
            except Exception as e:
                logger.error(f"❌ Failed to write batch {batch_id} to {table_name}: {e}")
                
        return write_batch    
    
    
    
    
    def run(self):
        """Run streaming pipeline"""
        try:
            logger.info("🚀 Starting Telecom Stream Processor...")
        
        # Create Spark session
            self.spark = self.create_spark_session()
        
        # Read from Kafka
            raw_stream = self.read_from_kafka()
        
        # Validate
            valid_stream = self.validate_and_filter(raw_stream)
        
        # Detect anomalies
            anomalies = self.detect_anomalies(valid_stream)
        
        # SIMPLE BATCH METRICS (no windowing, no state)
            batch_metrics = valid_stream.groupBy(
                "event_type", "region"
            ).agg(
                count("*").alias("count_events"),
                _sum(coalesce(col("duration_seconds"), lit(0))).alias("total_duration_seconds"),
                _sum(coalesce(col("data_mb"), lit(0))).alias("total_data_mb"),
                _sum(coalesce(col("amount"), lit(0))).alias("total_amount"),
                avg(col("duration_seconds")).alias("avg_duration_seconds"),
                avg(col("data_mb")).alias("avg_data_mb")
            ).select(
                current_timestamp().alias("window_start"),
                current_timestamp().alias("window_end"),
                lit("batch_metrics").alias("metric_type"),
                col("region"),
                col("event_type"),
                lit(None).cast("string").alias("event_subtype"),
                col("count_events"),
                col("total_duration_seconds"),
                (col("total_data_mb") / 1024.0).alias("total_data_gb"),
                col("total_amount"),
                col("avg_duration_seconds"),
                col("avg_data_mb"),
                current_timestamp().alias("created_at")
            )
        
        # Write anomalies
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
                .trigger(processingTime='5 seconds') \
                .start()

            logger.info("✅ Anomaly detection stream started")
        
        # Write batch metrics
            metrics_query = batch_metrics.writeStream \
                .foreachBatch(self.write_to_postgres(batch_metrics, "real_time_metrics")) \
                .outputMode("complete") \
                .trigger(processingTime='10 seconds') \
                .start()
        
            logger.info("✅ Batch metrics stream started")
        
        # Write raw events
            raw_events_query = valid_stream.writeStream \
                .foreachBatch(self.write_to_postgres(valid_stream, "raw_events")) \
                .outputMode("append") \
                .trigger(processingTime='5 seconds') \
                .start()
        
            logger.info("✅ Raw events stream started")
            logger.info("📊 All streams running!")
        
        # Wait
            self.spark.streams.awaitAnyTermination()
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stopping streams...")
        except Exception as e:
            logger.error(f"❌ Stream error: {e}", exc_info=True)
        finally:
            if self.spark:
                self.spark.stop()


def main():
    processor = TelecomStreamProcessor()
    processor.run()


if __name__ == '__main__':
    main()