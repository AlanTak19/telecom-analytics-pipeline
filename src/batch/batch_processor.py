"""
Batch Processing Job
Aggregates raw events into daily and hourly statistics
"""

import psycopg2
from datetime import datetime, timedelta
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchProcessor:
    """Process raw events into aggregated statistics"""
    
    def __init__(self, db_host="127.0.0.1", db_port=5432, 
                 db_name="telecom_analytics", db_user="telecom_user", 
                 db_password="SecurePassword123!"):
        """Initialize database connection"""
        self.conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        self.conn.autocommit = False
        logger.info("✅ Connected to database")
    
    def process_daily_stats(self, date=None):
        """Process daily statistics for a given date"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).date()
        
        logger.info(f"📊 Processing daily stats for {date}")
        
        cursor = self.conn.cursor()
        
        try:
            # Aggregate daily stats
            query = """
            INSERT INTO daily_stats (
                date, region, event_type,
                total_events, total_calls, total_sms, total_data_sessions,
                total_call_duration, avg_call_duration, max_call_duration,
                total_data_mb, avg_data_mb, max_data_mb,
                total_revenue, avg_transaction
            )
            SELECT 
                DATE(timestamp) as date,  -- Changed from created_at
                region,
                event_type,
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type = 'call') as total_calls,
                COUNT(*) FILTER (WHERE event_type = 'sms') as total_sms,
                COUNT(*) FILTER (WHERE event_type = 'data_session') as total_data_sessions,
                SUM(duration_seconds) FILTER (WHERE event_type = 'call') as total_call_duration,
                AVG(duration_seconds) FILTER (WHERE event_type = 'call') as avg_call_duration,
                MAX(duration_seconds) FILTER (WHERE event_type = 'call') as max_call_duration,
                SUM(data_mb) as total_data_mb,
                AVG(data_mb) as avg_data_mb,
                MAX(data_mb) as max_data_mb,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_transaction
            FROM raw_events
            WHERE DATE(timestamp) = %s  -- Changed from created_at
            GROUP BY DATE(timestamp), region, event_type  -- Changed from created_at
            """
            
            cursor.execute(query, (date,))
            rows_affected = cursor.rowcount
            self.conn.commit()
            
            logger.info(f"✅ Processed {rows_affected} daily stat records for {date}")
            return rows_affected
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error processing daily stats: {e}")
            raise
        finally:
            cursor.close()
    
    def process_hourly_stats(self, date=None):
        """Process hourly statistics"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).date()
        
        logger.info(f"📊 Processing hourly stats for {date}")
        
        cursor = self.conn.cursor()
        
        try:
            query = """
            INSERT INTO hourly_stats (
                hour_timestamp, region, event_type,
                total_events, total_duration, avg_duration,
                total_data_mb, total_amount
            )
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour_timestamp,  -- Changed from created_at
                region,
                event_type,
                COUNT(*) as total_events,
                SUM(COALESCE(duration_seconds, 0)) as total_duration,
                AVG(duration_seconds) as avg_duration,
                SUM(COALESCE(data_mb, 0)) as total_data_mb,
                SUM(COALESCE(amount, 0)) as total_amount
            FROM raw_events
            WHERE DATE(timestamp) = %s  -- Changed from created_at
            GROUP BY DATE_TRUNC('hour', timestamp), region, event_type  -- Changed from created_at

            """
            
            cursor.execute(query, (date,))
            rows_affected = cursor.rowcount
            self.conn.commit()
            
            logger.info(f"✅ Processed {rows_affected} hourly stat records for {date}")
            return rows_affected
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error processing hourly stats: {e}")
            raise
        finally:
            cursor.close()
    
    def get_summary(self):
        """Get summary of batch processing results"""
        cursor = self.conn.cursor()
        
        try:
            # Raw events count
            cursor.execute("SELECT COUNT(*) FROM raw_events")
            raw_count = cursor.fetchone()[0]
            
            # Daily stats
            cursor.execute("SELECT COUNT(*), MAX(date) FROM daily_stats")
            daily_count, latest_daily = cursor.fetchone()
            
            # Hourly stats
            cursor.execute("SELECT COUNT(*), MAX(hour_timestamp) FROM hourly_stats")
            hourly_count, latest_hourly = cursor.fetchone()
            
            summary = {
                'raw_events': raw_count,
                'daily_stats': daily_count,
                'latest_daily_date': latest_daily,
                'hourly_stats': hourly_count,
                'latest_hourly': latest_hourly
            }
            
            return summary
            
        finally:
            cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("✅ Database connection closed")


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Batch process telecom events')
    parser.add_argument('--date', type=str, help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--host', default='localhost', help='Database host')
    args = parser.parse_args()
    
    # Parse date if provided
    process_date = None
    if args.date:
        process_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    
    try:
        logger.info("🚀 Starting batch processing job")
        
        processor = BatchProcessor(db_host=args.host)
        
        # Process daily stats
        daily_rows = processor.process_daily_stats(process_date)
        
        # Process hourly stats
        hourly_rows = processor.process_hourly_stats(process_date)
        
        # Get summary
        summary = processor.get_summary()
        
        logger.info("=" * 50)
        logger.info("📊 BATCH PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Raw Events: {summary['raw_events']:,}")
        logger.info(f"Daily Stats: {summary['daily_stats']} records (Latest: {summary['latest_daily_date']})")
        logger.info(f"Hourly Stats: {summary['hourly_stats']} records (Latest: {summary['latest_hourly']})")
        logger.info(f"Processed {daily_rows} daily + {hourly_rows} hourly records")
        logger.info("=" * 50)
        logger.info("✅ Batch processing completed successfully!")
        
        processor.close()
        
    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}", exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()