"""
Simple Monitoring Dashboard
Displays real-time metrics from the database
"""

import psycopg2
import time
import os
from datetime import datetime
from tabulate import tabulate

class TelecomMonitor:
    """Monitor telecom analytics system"""
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '127.0.0.1'),  # Changed from localhost
            port=5432,
            database="telecom_analytics",
            user="telecom_user",
            password="SecurePassword123!"
        )
    
    def get_realtime_stats(self):
        """Get real-time event statistics"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE event_type = 'call') as calls,
                COUNT(*) FILTER (WHERE event_type = 'sms') as sms,
                COUNT(*) FILTER (WHERE event_type = 'data_session') as data_sessions,
                COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '5 minutes') as last_5min
            FROM raw_events
        """)
        return cursor.fetchone()
    
    def get_anomaly_stats(self):
        """Get anomaly statistics"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_anomalies,
                COUNT(*) FILTER (WHERE severity = 'high') as high_severity,
                COUNT(*) FILTER (WHERE detected_at >= NOW() - INTERVAL '1 hour') as last_hour
            FROM anomalies
        """)
        return cursor.fetchone()
    
    def get_top_regions(self):
        """Get top regions by event count"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                region,
                COUNT(*) as events
            FROM raw_events
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
            GROUP BY region
            ORDER BY events DESC
            LIMIT 5
        """)
        return cursor.fetchall()
    
    def get_system_health(self):
        """Get system health metrics"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                'Raw Events' as component,
                COUNT(*) as count,
                CASE WHEN COUNT(*) > 1000 THEN '✅' ELSE '⚠️' END as status
            FROM raw_events
            UNION ALL
            SELECT 'Daily Stats', COUNT(*), 
                   CASE WHEN COUNT(*) > 0 THEN '✅' ELSE '⚠️' END 
            FROM daily_stats
            UNION ALL
            SELECT 'Hourly Stats', COUNT(*), 
                   CASE WHEN COUNT(*) > 0 THEN '✅' ELSE '⚠️' END 
            FROM hourly_stats
        """)
        return cursor.fetchall()
    
    def display_dashboard(self):
        """Display monitoring dashboard"""
        os.system('clear' if os.name != 'nt' else 'cls')
        
        print("=" * 80)
        print("🔍 TELECOM ANALYTICS - MONITORING DASHBOARD")
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print()
        
        # Real-time Stats
        stats = self.get_realtime_stats()
        print("📊 REAL-TIME STATISTICS")
        print("-" * 80)
        print(f"Total Events: {stats[0]:,}")
        print(f"  - Calls: {stats[1]:,}")
        print(f"  - SMS: {stats[2]:,}")
        print(f"  - Data Sessions: {stats[3]:,}")
        print(f"Events (Last 5 min): {stats[4]:,}")
        print()
        
        # Anomalies
        anomalies = self.get_anomaly_stats()
        print("🚨 ANOMALY DETECTION")
        print("-" * 80)
        print(f"Total Anomalies: {anomalies[0]:,}")
        print(f"  - High Severity: {anomalies[1]:,}")
        print(f"  - Last Hour: {anomalies[2]:,}")
        print()
        
        # Top Regions
        regions = self.get_top_regions()
        print("🌍 TOP REGIONS (Last Hour)")
        print("-" * 80)
        print(tabulate(regions, headers=['Region', 'Events'], tablefmt='simple'))
        print()
        
        # System Health
        health = self.get_system_health()
        print("💚 SYSTEM HEALTH")
        print("-" * 80)
        print(tabulate(health, headers=['Component', 'Count', 'Status'], tablefmt='simple'))
        print()
        
        print("=" * 80)
        print("Press Ctrl+C to exit | Refreshing every 10 seconds...")
    
    def run(self, refresh_interval=10):
        """Run monitoring dashboard"""
        try:
            while True:
                self.display_dashboard()
                time.sleep(refresh_interval)
        except KeyboardInterrupt:
            print("\n\n✅ Monitoring stopped")
            self.conn.close()

if __name__ == '__main__':
    monitor = TelecomMonitor()
    monitor.run()