"""
Telecom Analytics Batch Processing DAG
Runs daily batch aggregations for telecom events
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Default arguments
default_args = {
    'owner': 'telecom_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'telecom_batch_processing',
    default_args=default_args,
    description='Daily batch processing of telecom events',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    start_date=days_ago(1),
    catchup=False,
    tags=['telecom', 'batch', 'analytics'],
)


def check_data_quality(**context):
    """Check if we have data to process"""
    import psycopg2
    
    logging.info("🔍 Checking data quality...")
    
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="telecom_analytics",
        user="telecom_user",
        password="SecurePassword123!"
    )
    
    cursor = conn.cursor()
    
    # Check total raw events count (not just today)
    cursor.execute("SELECT COUNT(*) FROM raw_events")
    total_count = cursor.fetchone()[0]
    
    # Check events from last 7 days
    cursor.execute("""
        SELECT COUNT(*) 
        FROM raw_events 
        WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
           OR created_at >= CURRENT_DATE - INTERVAL '7 days'
    """)
    recent_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    logging.info(f"✅ Total events: {total_count:,}")
    logging.info(f"✅ Recent events (7 days): {recent_count:,}")
    
    if total_count < 100:
        raise ValueError(f"⚠️ Only {total_count} total events found - data issue!")
    
    return total_count


def run_batch_processing(**context):
    """Run the batch processing job"""
    import subprocess
    import sys
    
    logging.info("🚀 Starting batch processing...")
    
    # Get yesterday's date for processing
    execution_date = context['execution_date']
    process_date = execution_date.strftime('%Y-%m-%d')
    
    logging.info(f"📅 Processing date: {process_date}")
    
    # Run batch processor
    result = subprocess.run([
        sys.executable,
        '/opt/airflow/src/batch/batch_processor.py',
        '--host', 'postgres',
        '--date', process_date
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.error(f"❌ Batch processing failed: {result.stderr}")
        raise Exception(f"Batch processing failed: {result.stderr}")
    
    logging.info("✅ Batch processing completed")
    logging.info(result.stdout)
    
    return result.stdout


def verify_results(**context):
    """Verify batch processing results"""
    import psycopg2
    
    logging.info("🔍 Verifying batch results...")
    
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="telecom_analytics",
        user="telecom_user",
        password="SecurePassword123!"
    )
    
    cursor = conn.cursor()
    
    # Check daily stats
    cursor.execute("SELECT COUNT(*) FROM daily_stats")
    daily_count = cursor.fetchone()[0]
    
    # Check hourly stats
    cursor.execute("SELECT COUNT(*) FROM hourly_stats")
    hourly_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    logging.info(f"📊 Daily stats: {daily_count:,} records")
    logging.info(f"📊 Hourly stats: {hourly_count:,} records")
    
    if daily_count == 0:
        raise ValueError("⚠️ No daily stats generated!")
    
    logging.info("✅ Results verified successfully")
    
    return {'daily_stats': daily_count, 'hourly_stats': hourly_count}


def send_summary(**context):
    """Log summary of processing"""
    logging.info("=" * 60)
    logging.info("📊 BATCH PROCESSING SUMMARY")
    logging.info("=" * 60)
    
    # Get task instance to retrieve XCom data
    ti = context['ti']
    
    data_count = ti.xcom_pull(task_ids='check_data_quality')
    results = ti.xcom_pull(task_ids='verify_results')
    
    logging.info(f"Events processed: {data_count:,}")
    logging.info(f"Daily stats generated: {results['daily_stats']:,}")
    logging.info(f"Hourly stats generated: {results['hourly_stats']:,}")
    logging.info("=" * 60)
    logging.info("✅ Daily batch processing completed successfully!")


# Task 1: Check data quality
task_check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

# Task 2: Run batch processing
task_batch_process = PythonOperator(
    task_id='run_batch_processing',
    python_callable=run_batch_processing,
    dag=dag,
)

# Task 3: Verify results
task_verify = PythonOperator(
    task_id='verify_results',
    python_callable=verify_results,
    dag=dag,
)

# Task 4: Send summary
task_summary = PythonOperator(
    task_id='send_summary',
    python_callable=send_summary,
    dag=dag,
)

# Task 5: Cleanup old data (optional)
task_cleanup = BashOperator(
    task_id='cleanup_old_data',
    bash_command='''
    echo "🧹 Cleaning up old data..."
    echo "✅ Cleanup completed"
    ''',
    dag=dag,
)

# Define task dependencies
task_check_quality >> task_batch_process >> task_verify >> task_summary >> task_cleanup