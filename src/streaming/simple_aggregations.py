"""
Simplified Real-time Aggregations WITHOUT Stateful Operations
Avoids checkpoint corruption by using simpler approach
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleStreamAggregator:
    """Simple streaming aggregations without stateful checkpoints"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SimpleStreamAggregator") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Database config
        self.db_url = "jdbc:postgresql://postgres:5432/telecom_analytics"
        self.db_user = "telecom_user"
        self.db_password = "SecurePassword123!"
        
        logger.info("✅ Spark session created")
    
    def read_from_kafka(self):
        """Read from Kafka"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "telecom_events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("msisdn", StringType()),
            StructField("event_type", StringType()),
            StructField("event_subtype", StringType()),
            StructField("duration_seconds", IntegerType()),
            StructField("data_mb", DoubleType()),
            StructField("amount", DoubleType()),
            StructField("region", StringType()),
            StructField("cell_tower_id", IntegerType()),
            StructField("timestamp", StringType())
        ])
        
        parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Fix timestamp
        parsed = parsed.withColumn(
            "event_time",
            coalesce(
                to_timestamp(col("timestamp")),
                current_timestamp()
            )
        )
        
        logger.info("✅ Kafka stream configured")
        return parsed
    
    def aggregate_5min_window(self, stream_df):
        """5-minute tumbling window aggregations"""
        
        # Add watermark for late data (1 minute)
        watermarked = stream_df.withWatermark("event_time", "1 minute")
        
        # 5-minute tumbling window by event_type
        aggregated = watermarked.groupBy(
            window(col("event_time"), "5 minutes"),
            col("event_type"),
            col("region")
        ).agg(
            count("*").alias("count_events"),
            sum(when(col("event_type") == "call", 
                    when(col("event_subtype") == "incoming", 1).otherwise(0))
               ).alias("incoming_calls"),
            sum(when(col("event_type") == "call",
                    when(col("event_subtype") == "outgoing", 1).otherwise(0))
               ).alias("outgoing_calls"),
            sum(when(col("event_type") == "sms", 1).otherwise(0)).alias("sms_count"),
            sum(when(col("event_type") == "data_session", 
                    coalesce(col("data_mb"), lit(0))).otherwise(0)
               ).alias("total_data_mb"),
            sum(when(col("event_type") == "balance_recharge",
                    coalesce(col("amount"), lit(0))).otherwise(0)
               ).alias("total_recharge_amount")
        )
        
        # Format for database
        result = aggregated.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("5min").alias("metric_type"),
            col("region"),
            col("event_type"),
            lit(None).cast("string").alias("event_subtype"),
            col("count_events"),
            lit(None).cast("bigint").alias("total_duration_seconds"),
            (col("total_data_mb") / 1024.0).alias("total_data_gb"),
            col("total_recharge_amount").alias("total_amount"),
            lit(None).cast("double").alias("avg_duration_seconds"),
            (col("total_data_mb") / col("count_events")).alias("avg_data_mb"),
            current_timestamp().alias("created_at")
        )
        
        return result
    
    def write_to_postgres(self, df, table_name):
        """Write stream to PostgreSQL"""
        
        def write_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", self.db_url) \
                        .option("dbtable", table_name) \
                        .option("user", self.db_user) \
                        .option("password", self.db_password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"✅ Wrote {batch_df.count()} records to {table_name} (batch {batch_id})")
            except Exception as e:
                logger.error(f"❌ Failed to write batch {batch_id}: {e}")
        
        return write_batch
    
    def run(self):
        """Run streaming aggregations"""
        try:
            logger.info("🚀 Starting simple stream aggregator...")
            
            # Read from Kafka
            stream = self.read_from_kafka()
            
            # 5-minute aggregations
            metrics_5min = self.aggregate_5min_window(stream)
            
            # Write to database
            query = metrics_5min.writeStream \
                .foreachBatch(self.write_to_postgres(metrics_5min, "real_time_metrics")) \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            logger.info("✅ 5-minute metrics stream started")
            logger.info("📊 Streaming aggregations running!")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stopping stream...")
        except Exception as e:
            logger.error(f"❌ Stream error: {e}", exc_info=True)
        finally:
            self.spark.stop()

if __name__ == '__main__':
    aggregator = SimpleStreamAggregator()
    aggregator.run()