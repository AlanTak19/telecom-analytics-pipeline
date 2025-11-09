CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    region VARCHAR(50),
    event_type VARCHAR(50),
    
    -- Aggregated metrics
    total_events BIGINT,
    total_calls BIGINT,
    total_sms BIGINT,
    total_data_sessions BIGINT,
    
    -- Duration metrics (seconds)
    total_call_duration BIGINT,
    avg_call_duration DECIMAL(10,2),
    max_call_duration INTEGER,
    
    -- Data metrics (MB)
    total_data_mb DECIMAL(15,2),
    avg_data_mb DECIMAL(10,2),
    max_data_mb DECIMAL(10,2),
    
    -- Financial metrics (tenge)
    total_revenue DECIMAL(15,2),
    avg_transaction DECIMAL(10,2),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(date, region, event_type)
);

CREATE INDEX idx_daily_stats_date ON daily_stats(date);
CREATE INDEX idx_daily_stats_region ON daily_stats(region);
CREATE INDEX idx_daily_stats_type ON daily_stats(event_type);

-- Also create hourly stats table
CREATE TABLE IF NOT EXISTS hourly_stats (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    region VARCHAR(50),
    event_type VARCHAR(50),
    
    total_events BIGINT,
    total_duration BIGINT,
    avg_duration DECIMAL(10,2),
    total_data_mb DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(hour_timestamp, region, event_type)
);

CREATE INDEX idx_hourly_stats_timestamp ON hourly_stats(hour_timestamp);

\q