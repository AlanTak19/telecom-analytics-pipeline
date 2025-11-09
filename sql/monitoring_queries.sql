-- ============================================
-- MONITORING QUERIES FOR TELECOM ANALYTICS
-- ============================================

-- 1. Real-time Event Count (Last 5 minutes)
SELECT 
    COUNT(*) as events_last_5min,
    COUNT(*) FILTER (WHERE event_type = 'call') as calls,
    COUNT(*) FILTER (WHERE event_type = 'sms') as sms,
    COUNT(*) FILTER (WHERE event_type = 'data_session') as data_sessions
FROM raw_events
WHERE timestamp >= NOW() - INTERVAL '5 minutes';

-- 2. Events by Region (Last Hour)
SELECT 
    region,
    COUNT(*) as event_count,
    COUNT(DISTINCT msisdn) as unique_users
FROM raw_events
WHERE timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY region
ORDER BY event_count DESC;

-- 3. Anomaly Summary (Last 24 Hours)
SELECT 
    anomaly_type,
    COUNT(*) as anomaly_count,
    AVG(CASE 
        WHEN severity = 'high' THEN 3
        WHEN severity = 'medium' THEN 2
        ELSE 1
    END) as avg_severity
FROM anomalies
WHERE detected_at >= NOW() - INTERVAL '24 hours'
GROUP BY anomaly_type
ORDER BY anomaly_count DESC;

-- 4. Daily Stats Summary (Last 7 Days)
SELECT 
    date,
    SUM(total_events) as total_events,
    SUM(total_calls) as total_calls,
    SUM(total_sms) as total_sms,
    SUM(total_data_sessions) as total_data_sessions,
    ROUND(SUM(total_data_mb) / 1024.0, 2) as total_data_gb
FROM daily_stats
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date
ORDER BY date DESC;

-- 5. System Health Check
SELECT 
    'Raw Events' as metric,
    COUNT(*) as value
FROM raw_events
UNION ALL
SELECT 'Daily Stats', COUNT(*) FROM daily_stats
UNION ALL
SELECT 'Hourly Stats', COUNT(*) FROM hourly_stats
UNION ALL
SELECT 'Anomalies', COUNT(*) FROM anomalies
UNION ALL
SELECT 'Real-time Metrics', COUNT(*) FROM real_time_metrics;

-- 6. Peak Usage Hours (Today)
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour,
    COUNT(*) as event_count,
    COUNT(DISTINCT msisdn) as unique_users
FROM raw_events
WHERE DATE(timestamp) = CURRENT_DATE
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour;

-- 7. Top Regions by Data Usage (Last 24 Hours)
SELECT 
    region,
    COUNT(*) as sessions,
    ROUND(SUM(data_mb), 2) as total_data_mb,
    ROUND(AVG(data_mb), 2) as avg_data_mb
FROM raw_events
WHERE event_type = 'data_session'
  AND timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY region
ORDER BY total_data_mb DESC
LIMIT 10;

-- 8. Call Duration Statistics (Last 24 Hours)
SELECT 
    ROUND(AVG(duration_seconds) / 60.0, 2) as avg_call_minutes,
    ROUND(MAX(duration_seconds) / 60.0, 2) as max_call_minutes,
    COUNT(*) as total_calls
FROM raw_events
WHERE event_type = 'call'
  AND timestamp >= NOW() - INTERVAL '24 hours';