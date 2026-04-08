# Pipeline Health Dashboard — Metabase Setup

This document describes the recommended **Pipeline Health Dashboard** for monitoring data pipeline health and observability.

## Overview

The dashboard provides real-time visibility into:
- Pipeline run success/failure rates
- Task failure trends
- Data volume processed per run
- Most recent run status per DAG
- Historical pipeline health

Built on the `nyc_taxi.pipeline_run_log` table populated by Airflow failure callbacks and success logging.

## Prerequisites

1. **Metabase running** (docker-compose up or standalone)
2. **Connected to PostgreSQL warehouse** via the metabase.toml connection
3. **pipeline_run_log table created** (auto-initialized by `datawarehouse_service/init/01_schema.sql`)
4. **At least one pipeline run** (populate with: `docker-compose exec airflow-webserver airflow dags trigger yellow_monthly_dump -c file_month=2025-01`)

## Dashboard Setup (Manual Steps in Metabase UI)

### 1. Create Dashboard
1. Click **+ New Dashboard**
2. Name: `Pipeline Health`
3. Click **Create**

### 2. Add Charts

#### Chart 1: Pipeline Runs Per Day (Bar Chart)
**Query:**
```sql
SELECT
  DATE(logged_at) AS run_date,
  COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success,
  COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
  COUNT(*) as total
FROM nyc_taxi.pipeline_run_log
WHERE logged_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(logged_at)
ORDER BY run_date DESC
```

**Chart Type:** Stacked Bar
**X-axis:** run_date
**Y-axis:** success (green), failed (red)

---

#### Chart 2: Task Failure Rate Over Time (Line Chart)
**Query:**
```sql
SELECT
  DATE_TRUNC('day', logged_at) AS run_day,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'FAILED')
    / NULLIF(COUNT(*), 0),
    1
  ) AS failure_rate_pct
FROM nyc_taxi.pipeline_run_log
WHERE logged_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', logged_at)
ORDER BY run_day DESC
```

**Chart Type:** Line
**X-axis:** run_day
**Y-axis:** failure_rate_pct

---

#### Chart 3: Most Recent Runs (Table)
**Query:**
```sql
SELECT
  dag_id,
  task_id,
  DATE_TRUNC('second', execution_date) AS exec_time,
  status,
  CASE
    WHEN error_message IS NOT NULL THEN LEFT(error_message, 100)
    ELSE '—'
  END AS error_summary,
  DATE_PART('minute', NOW() - logged_at) AS minutes_ago
FROM nyc_taxi.pipeline_run_log
WHERE logged_at >= NOW() - INTERVAL '7 days'
ORDER BY logged_at DESC
LIMIT 50
```

**Chart Type:** Table
**Sorting:** logged_at DESC

---

#### Chart 4: Last Successful Run Per DAG (KPI Cards)
**Query 1: yellow_monthly_dump**
```sql
SELECT
  COALESCE(
    DATE_PART('day', NOW() - MAX(logged_at))::INT,
    -1
  ) AS days_since_success
FROM nyc_taxi.pipeline_run_log
WHERE dag_id = 'yellow_monthly_dump' AND status = 'SUCCESS'
```

**Query 2: taxi_data_pipeline**
```sql
SELECT
  COALESCE(
    DATE_PART('day', NOW() - MAX(logged_at))::INT,
    -1
  ) AS days_since_success
FROM nyc_taxi.pipeline_run_log
WHERE dag_id = 'taxi_data_pipeline' AND status = 'SUCCESS'
```

**Query 3: monthly_archival_pipeline**
```sql
SELECT
  COALESCE(
    DATE_PART('day', NOW() - MAX(logged_at))::INT,
    -1
  ) AS days_since_success
FROM nyc_taxi.pipeline_run_log
WHERE dag_id = 'monthly_archival_pipeline' AND status = 'SUCCESS'
```

**Chart Type:** Scalar
**Display:** Large KPI card showing days since last success
**Conditional Formatting:** Green if < 1 day, Yellow if 1-7 days, Red if > 7 days

---

#### Chart 5: Failure Messages (Drill-Down Table)
**Query:**
```sql
SELECT
  logged_at,
  dag_id,
  task_id,
  status,
  error_message,
  COUNT(*) OVER (PARTITION BY task_id, DATE(logged_at)) AS failures_today
FROM nyc_taxi.pipeline_run_log
WHERE status = 'FAILED'
  AND logged_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY logged_at DESC
LIMIT 100
```

**Chart Type:** Table
**Sorting:** logged_at DESC

---

## Interpreting the Dashboard

### Green (Healthy)
- Zero failures in the last day
- All DAGs show "< 1 day" since last success
- Bar chart shows mostly green bars

### Yellow (Warning)
- Sporadic failures (< 20% failure rate)
- Last success was 1-7 days ago
- Investigate: check most recent run error messages

### Red (Action Required)
- > 20% failure rate in recent days
- Any DAG showing "no successful runs"
- Last success > 7 days ago
- Action: check Airflow logs, verify data availability, restart pipeline

## Typical Failure Scenarios & Debugging

### "zero rows found for {file_month}"
→ Data file doesn't exist in `data/yellow_taxi/` or `data/green_taxi/`
→ Action: Upload parquet file with correct month format

### "no rows with NULL pickup/dropoff datetime"
→ Data quality issue in source parquet
→ Action: Review source data, update data cleaning logic

### "Unable to ingest file to Postgres"
→ PostgreSQL connection failed, or table schema mismatch
→ Action: Check postgres-warehouse container health, verify schema in 01_schema.sql

### "gold table is empty"
→ Silver→Gold transform produced no rows
→ Action: Check CASE statement logic in build_gold, verify silver table has data

### "preflight_check: Last success > 35 days"
→ Pipeline has not run in > 35 days
→ Action: Run yellow_monthly_dump or taxi_data_pipeline manually

## Automated Alerting (Future Enhancement)

Metabase can send email/Slack alerts if:
- Failure rate > 50% on any DAG
- Days since last success > 7 for any DAG
- Any task failure in the last hour

Set up in Metabase UI: Dashboard → ... → Subscriptions

## Data Retention

The `pipeline_run_log` table has no TTL. For long-running production:
- Monthly archive old rows: `INSERT INTO pipeline_run_log_archive SELECT * WHERE logged_at < ...`
- Or set a Postgres job: `DELETE FROM pipeline_run_log WHERE logged_at < NOW() - INTERVAL '1 year'`

## References

- Pipeline Run Log Schema: `datawarehouse_service/init/01_schema.sql`
- Failure Callback: `airflow_service/dags/*.py` → `_on_failure_callback()`
- Data Quality Checks: `airflow_service/dags/*.py` → `validate_bronze()`, `validate_gold()`
