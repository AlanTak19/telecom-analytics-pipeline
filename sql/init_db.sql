-- ============================================
-- TELECOM ANALYTICS DATABASE INITIALIZATION
-- ============================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- TABLE: raw_events
-- ============================================
CREATE TABLE IF NOT EXISTS raw_events (
    event_id UUID PRIMARY KEY,
    msisdn VARCHAR(20) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_subtype VARCHAR(50),
    duration_seconds INTEGER,
    data_mb DECIMAL(10, 2),
    amount DECIMAL(10, 2),
    region VARCHAR(100),
    cell_tower_id INTEGER,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_event_type CHECK (event_type IN ('call', 'sms', 'data_session', 'balance_recharge', 'service_activation')),
    CONSTRAINT check_positive_duration CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
    CONSTRAINT check_positive_data CHECK (data_mb IS NULL OR data_mb >= 0),
    CONSTRAINT check_positive_amount CHECK (amount IS NULL OR amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_region ON raw_events(region);
CREATE INDEX IF NOT EXISTS idx_raw_events_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_msisdn ON raw_events(msisdn);

-- ============================================
-- TABLE: real_time_metrics
-- ============================================
CREATE TABLE IF NOT EXISTS real_time_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    metric_type VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    event_type VARCHAR(50),
    event_subtype VARCHAR(50),
    count_events BIGINT DEFAULT 0,
    total_duration_seconds BIGINT DEFAULT 0,
    total_data_gb DECIMAL(15, 4) DEFAULT 0,
    total_amount DECIMAL(15, 2) DEFAULT 0,
    avg_duration_seconds DECIMAL(10, 2),
    avg_data_mb DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (window_start, window_end, metric_type, region, event_type, event_subtype)
);

CREATE INDEX IF NOT EXISTS idx_rt_metrics_window ON real_time_metrics(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_rt_metrics_type ON real_time_metrics(metric_type);
CREATE INDEX IF NOT EXISTS idx_rt_metrics_region ON real_time_metrics(region);

-- ============================================
-- TABLE: anomalies
-- ============================================
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    msisdn VARCHAR(20),
    anomaly_type VARCHAR(100) NOT NULL,
    anomaly_reason TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium',
    event_data JSONB,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_severity CHECK (severity IN ('low', 'medium', 'high', 'critical'))
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies(detected_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomalies);

-- ============================================
-- TABLE: daily_stats
-- ============================================
CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL,
    processing_date DATE NOT NULL,
    metric_category VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    event_type VARCHAR(50),
    total_events BIGINT DEFAULT 0,
    total_duration_hours DECIMAL(15, 2) DEFAULT 0,
    total_data_tb DECIMAL(15, 6) DEFAULT 0,
    total_recharge_amount DECIMAL(18, 2) DEFAULT 0,
    avg_call_duration_minutes DECIMAL(10, 2),
    avg_data_per_user_mb DECIMAL(10, 2),
    unique_users BIGINT DEFAULT 0,
    peak_hour INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, processing_date)
) PARTITION BY RANGE (processing_date);

-- Create partitions
CREATE TABLE IF NOT EXISTS daily_stats_2024_11 PARTITION OF daily_stats
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE IF NOT EXISTS daily_stats_2024_12 PARTITION OF daily_stats
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS daily_stats_2025_01 PARTITION OF daily_stats
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- ============================================
-- TABLE: pipeline_monitoring
-- ============================================
CREATE TABLE IF NOT EXISTS pipeline_monitoring (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    pipeline_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_processed BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    processing_time_seconds INTEGER,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_status CHECK (status IN ('running', 'completed', 'failed', 'pending'))
);

CREATE INDEX IF NOT EXISTS idx_monitoring_pipeline ON pipeline_monitoring(pipeline_name, pipeline_type);
CREATE INDEX IF NOT EXISTS idx_monitoring_status ON pipeline_monitoring(status);

-- ============================================
-- SUCCESS MESSAGE
-- ============================================
DO $$
BEGIN
    RAISE NOTICE '✅ Database initialization completed successfully!';
END $$;