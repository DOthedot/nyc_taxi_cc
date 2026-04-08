-- ============================================================
-- NYC Taxi Pipeline — Database Schema Initialization
--
-- This file is auto-executed by PostgreSQL on first container start
-- via /docker-entrypoint-initdb.d/ mount
-- ============================================================

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS nyc_taxi;

-- ============================================================
-- BRONZE LAYER (Raw Data)
-- ============================================================
-- Bronze tables are created by Airflow DAGs via to_sql()
-- Schemas below are created empty for reference

-- ============================================================
-- ZONE LOOKUP (Dimension Table)
-- ============================================================
CREATE TABLE IF NOT EXISTS nyc_taxi.bronze_taxi_zone_lookup (
    LocationID INTEGER PRIMARY KEY,
    Borough TEXT,
    Zone TEXT,
    service_zone TEXT
);

-- ============================================================
-- GREEN TAXI: SILVER & GOLD LAYERS
-- ============================================================

-- Silver layer: Deduplicated, enriched, with zone lookup
CREATE TABLE IF NOT EXISTS nyc_taxi.silver_green_taxi_data (
    vendor_id INTEGER,
    borough TEXT NOT NULL DEFAULT 'Unknown',
    service_zone TEXT NOT NULL DEFAULT 'Unknown',
    pickup_date DATE NOT NULL,
    mnth SMALLINT NOT NULL,
    trip_time NUMERIC(8, 2),
    passenger_count INTEGER NOT NULL DEFAULT 0,
    trip_distance DOUBLE PRECISION,
    trip_amount NUMERIC(10, 2),
    payment_type INTEGER NOT NULL DEFAULT 5
);

-- Indexes for Metabase BI queries
CREATE INDEX IF NOT EXISTS idx_silver_green_pickup_date ON nyc_taxi.silver_green_taxi_data (pickup_date);
CREATE INDEX IF NOT EXISTS idx_silver_green_mnth ON nyc_taxi.silver_green_taxi_data (mnth);
CREATE INDEX IF NOT EXISTS idx_silver_green_borough ON nyc_taxi.silver_green_taxi_data (borough);
CREATE INDEX IF NOT EXISTS idx_silver_green_service_zone ON nyc_taxi.silver_green_taxi_data (service_zone);

-- Gold layer: Human-readable vendor and payment type labels
CREATE TABLE IF NOT EXISTS nyc_taxi.gold_green_taxi_data (
    vendor_id TEXT,
    borough TEXT NOT NULL DEFAULT 'Unknown',
    service_zone TEXT NOT NULL DEFAULT 'Unknown',
    pickup_date DATE NOT NULL,
    mnth SMALLINT NOT NULL,
    trip_time NUMERIC(8, 2),
    passenger_count INTEGER NOT NULL DEFAULT 0,
    trip_distance DOUBLE PRECISION,
    trip_amount NUMERIC(10, 2),
    payment_type TEXT
);

-- Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_gold_green_pickup_date ON nyc_taxi.gold_green_taxi_data (pickup_date);
CREATE INDEX IF NOT EXISTS idx_gold_green_mnth ON nyc_taxi.gold_green_taxi_data (mnth);
CREATE INDEX IF NOT EXISTS idx_gold_green_borough ON nyc_taxi.gold_green_taxi_data (borough);
CREATE INDEX IF NOT EXISTS idx_gold_green_service_zone ON nyc_taxi.gold_green_taxi_data (service_zone);

-- ============================================================
-- YELLOW TAXI: SILVER & GOLD LAYERS
-- ============================================================

-- Silver layer: Deduplicated, enriched, with zone lookup
CREATE TABLE IF NOT EXISTS nyc_taxi.silver_yellow_taxi_data (
    vendor_id INTEGER,
    borough TEXT NOT NULL DEFAULT 'Unknown',
    service_zone TEXT NOT NULL DEFAULT 'Unknown',
    pickup_date DATE NOT NULL,
    mnth SMALLINT NOT NULL,
    trip_time NUMERIC(8, 2),
    passenger_count INTEGER NOT NULL DEFAULT 0,
    trip_distance DOUBLE PRECISION,
    trip_amount NUMERIC(10, 2),
    payment_type INTEGER NOT NULL DEFAULT 5
);

-- Indexes for Metabase BI queries
CREATE INDEX IF NOT EXISTS idx_silver_yellow_pickup_date ON nyc_taxi.silver_yellow_taxi_data (pickup_date);
CREATE INDEX IF NOT EXISTS idx_silver_yellow_mnth ON nyc_taxi.silver_yellow_taxi_data (mnth);
CREATE INDEX IF NOT EXISTS idx_silver_yellow_borough ON nyc_taxi.silver_yellow_taxi_data (borough);
CREATE INDEX IF NOT EXISTS idx_silver_yellow_service_zone ON nyc_taxi.silver_yellow_taxi_data (service_zone);

-- Gold layer: Human-readable vendor and payment type labels
CREATE TABLE IF NOT EXISTS nyc_taxi.gold_yellow_taxi_data (
    vendor_id TEXT,
    borough TEXT NOT NULL DEFAULT 'Unknown',
    service_zone TEXT NOT NULL DEFAULT 'Unknown',
    pickup_date DATE NOT NULL,
    mnth SMALLINT NOT NULL,
    trip_time NUMERIC(8, 2),
    passenger_count INTEGER NOT NULL DEFAULT 0,
    trip_distance DOUBLE PRECISION,
    trip_amount NUMERIC(10, 2),
    payment_type TEXT
);

-- Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_gold_yellow_pickup_date ON nyc_taxi.gold_yellow_taxi_data (pickup_date);
CREATE INDEX IF NOT EXISTS idx_gold_yellow_mnth ON nyc_taxi.gold_yellow_taxi_data (mnth);
CREATE INDEX IF NOT EXISTS idx_gold_yellow_borough ON nyc_taxi.gold_yellow_taxi_data (borough);
CREATE INDEX IF NOT EXISTS idx_gold_yellow_service_zone ON nyc_taxi.gold_yellow_taxi_data (service_zone);

-- ============================================================
-- PIPELINE OBSERVABILITY
-- ============================================================

-- Log of pipeline run status for monitoring
CREATE TABLE IF NOT EXISTS nyc_taxi.pipeline_run_log (
    id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    execution_date TIMESTAMPTZ,
    status TEXT NOT NULL,  -- 'SUCCESS' | 'FAILED'
    error_message TEXT,
    row_count INTEGER,
    logged_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_dag_id ON nyc_taxi.pipeline_run_log (dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_status ON nyc_taxi.pipeline_run_log (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_logged_at ON nyc_taxi.pipeline_run_log (logged_at);
