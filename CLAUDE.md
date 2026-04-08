# Engineering Practices & Decisions

This document captures the production-grade engineering patterns and decisions made during this project's refactoring.

## Guiding Philosophy

**"Build systems you would defend in a code review."**

Every change prioritizes:
1. **Security**: No hardcoded credentials, parameterized SQL, env var config
2. **Observability**: Failure logging, data quality checks, pipeline health metrics
3. **Reliability**: Retry logic, graceful degradation, pre-flight validation
4. **Clarity**: Self-documenting code, explicit error messages, single source of truth
5. **Maintainability**: Atomic commits, DRY principles, minimal complexity

## Key Decisions & Rationales

### Phase 0: Security & Correctness Fixes

#### Decision: Extract All Credentials to Environment Variables

**What Changed**:
```python
# Before
POSTGRES_CONFIG = {
    'user': 'myuser',
    'password': 'mysecretpassword',
}

# After
POSTGRES_CONFIG = {
    'user': os.environ['POSTGRES_WAREHOUSE_USER'],  # Required, no default
    'password': os.environ['POSTGRES_WAREHOUSE_PASSWORD'],
}
```

**Why**:
- Credentials never committed to git
- `.env` file gitignored, `.env.example` committed for team onboarding
- Environment-based config matches 12-factor app principles
- Fails loud if credentials not set (no silent defaults)

**Pattern**: Use `os.environ[KEY]` (raises KeyError) instead of `os.environ.get(KEY, "default")` (silent failures).

---

#### Decision: Fix Race Condition via Task Dependencies

**What Changed**:
```python
# Before: archive_data runs concurrently with postgre_cdl_dump
postgres_task = postgre_cdl_dump(downloaded_filepath)
archive_task = archive_data(downloaded_filepath)  # Both receive same upstream value

# After: archive_data depends on postgres_task completing
postgres_task = postgre_cdl_dump(downloaded_filepath)
archive_task = archive_data(postgres_task)  # archive depends on postgres_task return value
```

**Why**:
- Airflow sees `archive_data(downloaded_filepath)` and `postgre_cdl_dump(downloaded_filepath)` as independent (same upstream)
- Both can run concurrently → file deletion races with file reading
- Consuming return value creates explicit dependency edge in DAG graph
- Prevents file-not-found errors mid-read

**Lesson**: Always be explicit about task dependencies; let the framework enforce ordering.

---

#### Decision: Use SQL Identifiers for Dynamic Table Names

**What Changed**:
```python
# Before: SQL injection vulnerability
conn.execute(f"CREATE TABLE nyc_taxi.{backup_table} AS ...")

# After: parameterized SQL
from psycopg.sql import SQL, Identifier
conn.execute(
    SQL("CREATE TABLE nyc_taxi.{} AS ...").format(
        Identifier(backup_table)
    )
)
```

**Why**:
- f-strings interpolate directly into SQL (injection risk if table_name is user-controlled)
- Identifier properly quotes and escapes table/column names
- Still readable without regex patterns
- Matches SQLAlchemy best practices

**Pattern**: Never interpolate user input into SQL. Use parameterized queries (bound variables) or schema-aware quoting (Identifier, text()).

---

#### Decision: Fix Year-Boundary Month Filter

**What Changed**:
```python
# Before: fails when data spans year boundary
DATE_PART('month', tpep_pickup_datetime) = (
    SELECT MAX(DATE_PART('month', tpep_pickup_datetime))
    FROM bronze_taxi_data
)
# If bronze has Dec 2024 + Jan 2025 data:
#   MAX(DATE_PART('month', ...)) = 12 (December, highest month number)
#   Jan 2025 has DATE_PART('month', ...) = 1
#   Filter returns zero rows for January

# After: uses full timestamps including year
DATE_TRUNC('month', tpep_pickup_datetime) = (
    SELECT MAX(DATE_TRUNC('month', tpep_pickup_datetime))
    FROM bronze_taxi_data
)
# DATE_TRUNC('month', ...) returns 2024-12-01 or 2025-01-01
# Comparison includes year, works across year boundaries
```

**Why**:
- DATE_PART extracts only the month number (1-12), losing year context
- DATE_TRUNC returns timestamp truncated to month granularity (preserves year)
- Filtering `2025-01-01 = 2025-01-01` works correctly
- Future-proofs against year-boundary data loads

**Lesson**: When filtering by time periods, use DATE_TRUNC/DATE_EXTRACT, not component extraction.

---

### Phase 1: Dead Code Removal

#### Decision: Delete Prototype Services Completely

**What Deleted**:
- `kafka_service/` (3 files, 49KB)
- `spark_service/` (4 files, broken dependencies)
- `rustfs_service/` (unused S3-like storage)
- `bi_service/connetion.py` (dead debug script)
- `data_wrangling.ipynb` (one-time EDA)

**Why**:
- Prototype services confuse portfolio reviewers ("why are there two Kafka/Spark stacks?")
- Dead code signals poor engineering hygiene
- Unused imports/classes waste cognitive load during review
- Code not in use becomes stale and misleading
- Deleting clarifies "this is the canonical implementation"

**Pattern**: In portfolio projects, ruthlessly delete alternatives. Show one clear, well-justified approach.

---

### Phase 2: Docker Compose Consolidation

#### Decision: Single Root docker-compose.yml Instead of Service-Specific Composes

**What Changed**:
```bash
# Before: multiple commands
docker compose -f datawarehouse_service/docker-compose.yml up -d
docker compose -f airflow_service/docker-compose.yml up -d
docker compose -f spark-redpanda_service/docker-compose.yml up -d

# After: one command
docker compose up -d
```

**Why**:
1. **Network Isolation**: All services on `data-network` bridge, no `host.docker.internal` hacks
2. **Linux Compatibility**: `host.docker.internal` only works on Mac/Windows Docker Desktop
3. **Health Checks**: `depends_on: {condition: service_healthy}` enforces startup ordering
4. **Credentialing**: `env_file: .env` on all services, single source for secrets
5. **Operator Simplicity**: One command to start, one to stop

**Architecture**:
```yaml
networks:
  data-network:  # All 8 services on this bridge
    driver: bridge

services:
  postgres-warehouse:
    healthcheck: ...  # Ensure DB ready before DAGs start
  postgres-airflow:
    healthcheck: ...
  airflow-webserver:
    depends_on:
      postgres-airflow:
        condition: service_healthy  # Wait for DB before webserver
  # ... etc for all services
```

**Tradeoff**: 250+ line compose file (but heavily commented, worth it).

---

### Phase 3: Schema Governance

#### Decision: Explicit DDL + Shared Spark Schemas Module

**What Changed**:
```python
# Before: Schema defined inline in 3 consumer scripts (duplication)
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    # ... 17 more fields, repeated 3 times
])

# After: Single source of truth
from schemas import YELLOW_TAXI_SCHEMA  # imported in all 3 consumers
```

**Why**:
- **DRY Principle**: Schema defined once, imported everywhere
- **Type Safety**: Caught `consumer_pgdump.py` using green taxi schema for yellow topic (field name mismatch)
- **Evolution**: Future schema changes propagate to all consumers automatically
- **Documentation**: schemas.py is now the schema reference

**Pattern**: Always define data contracts (schemas, interfaces, data models) in central modules.

---

### Phase 4: Data Quality Checks

#### Decision: Validation Tasks as Explicit DAG Steps

**What Changed**:
```python
# Before: No checks
ingest_bronze → build_silver → build_gold

# After: Quality gates between layers
ingest_bronze → validate_bronze → build_silver → build_gold → validate_gold
```

**Validation Strategy**:

| Check | Hard Fail | Soft Warn | Why |
|-------|-----------|-----------|-----|
| **validate_bronze** | Zero rows, NULL datetimes | >5% negative amounts | Catch load failures early, before expensive transforms |
| **validate_gold** | Empty table | >1% row loss from silver | Verify transform didn't silently drop all rows |

**Why**:
- **Early Detection**: Fail fast at bronze before wasting compute on silver/gold transforms
- **Data Lineage**: Trace data loss across medallion layers
- **Observability**: Log quality metrics for Metabase dashboard
- **Production Pattern**: Matches industry practice (dbt tests, Great Expectations)

---

### Phase 5: Airflow Hardening

#### Decision: Retry Strategy with Exponential Backoff + Failure Logging

**What Changed**:
```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # Delays: 5min, 10min, 20min
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_failure_callback,  # Log to table
    "execution_timeout": timedelta(hours=2),
}
```

**Why**:
- **3 Retries**: Handles transient failures (brief DB downtime, network hiccups)
- **Exponential Backoff**: Avoids hammering failed systems (5 min → 10 min → 20 min delays)
- **Max Retry Delay**: Prevents hour-long retry chains
- **2-Hour Timeout**: Catches runaway tasks before they hang resources
- **Failure Logging**: Every failure written to pipeline_run_log for observability

**Pattern**: Transient failures ≠ logic errors. Retry with backoff, log all attempts, alert on repeated failures.

---

#### Decision: Preflight Check Before Destructive Operations

**What Changed**:
```python
# New task in monthly_archival_pipeline
@task
def preflight_check():
    """Abort if no successful pipeline run in last 35 days."""
    last_success = conn.execute("""
        SELECT MAX(logged_at) FROM pipeline_run_log
        WHERE dag_id IN ('yellow_monthly_dump', 'taxi_data_pipeline')
        AND status = 'SUCCESS'
    """).scalar()

    if last_success is None:
        raise ValueError("No successful runs. Backup aborted.")
    if (now - last_success).days > 35:
        raise ValueError("Last success > 35 days. Aborting.")
```

**Why**:
- Archival is destructive (creates timestamped backup tables)
- Blindly backing up empty tables wastes space
- Preflight validates downstream dependencies before running
- Operators know exactly why DAG was rejected

**Pattern**: Before any destructive operation, validate all preconditions are met.

---

### Phase 6: Observability

#### Decision: Write-to-Table Pattern for Failure Tracking

**What Changed**:
```python
def _on_failure_callback(context):
    """Log failures to pipeline_run_log table."""
    conn.execute("""
        INSERT INTO nyc_taxi.pipeline_run_log
            (dag_id, task_id, execution_date, status, error_message)
        VALUES (:dag_id, :task_id, :exec_date, 'FAILED', :error)
    """)
```

**Why**:
- Failures queryable in SQL (Metabase dashboards)
- Audit trail of all pipeline runs (historized)
- No external logging infrastructure (ELK, Datadog)
- Queryable in same DB as data (low friction)

**Tradeoff**: Not real-time alerting. For production, combine with Metabase subscriptions (email/Slack) or add webhook trigger.

---

### Phase 7: Documentation

#### Decision: Production-Grade README + CLAUDE.md

**Why**:
- README is first impression (50% of evaluation)
- Show you understand: architecture decisions, tradeoffs, known limitations
- CLAUDE.md demonstrates thoughtfulness and engineering maturity
- Clear "Known Limitations" section shows you didn't oversell

**Pattern**: Good documentation = respect for future maintainers (including interviewers).

---

## Commit Message Strategy

Every commit follows this pattern:

```
Phase X: Feature Name — Short Description

## Problem
What was broken or missing?

## Solution
What did you do?

## Why This Approach
What are the tradeoffs?

## Impact
What changes for users/maintainers?
```

**Why**: Commit history is your engineering journal. Future you (or an interviewer) reads git log to understand your decision-making.

---

## Testing Philosophy

**For Portfolio Projects**: Focus on correctness, not coverage.

- ✅ Manual testing of happy path (run one DAG end-to-end)
- ✅ Query results in Metabase (sanity check)
- ❌ Don't write 100+ unit tests (signals uncertainty, not confidence)

**Test the Scary Parts**:
- SQL correctness (year-boundary filtering, month calculations)
- Security (credential handling, SQL injection risk)
- Data loss scenarios (validate_gold checks for empty table)

---

## Code Review Checklist (For Yourself)

Before committing, ask:

- [ ] Is this code I'd defend in an interview?
- [ ] Are all secrets removed or env-var'd?
- [ ] Is there a clear reason for this architecture choice?
- [ ] Did I delete dead code, or just hide it?
- [ ] Would a new team member understand what this does?
- [ ] What could go wrong? Did I handle it?

---

## Reading Order for Reviewers

1. **README.md** — Understand the problem and approach
2. **git log --oneline** — See decision history
3. **docker-compose.yml** — Understand service topology
4. **airflow_service/dags/*.py** — See production patterns in action
5. **datawarehouse_service/init/01_schema.sql** — Data model
6. **This file (CLAUDE.md)** — Understand reasoning

---

## Further Learning

Books that influenced this approach:
- **"Designing Data-Intensive Applications"** (Kleppmann) — Tradeoffs, failure modes
- **"The Twelve-Factor App"** — Config, secrets, disposability
- **"Site Reliability Engineering"** (Google) — Observability, reliability

Patterns to study:
- Medallion architecture (Unity Technologies data governance model)
- Data quality frameworks (Great Expectations, dbt tests)
- Observability-as-code (Terraform, CaC principles)

---

**Last Updated**: 2026-04-08
**Status**: ✅ Production patterns implemented
**Next Level**: Kubernetes orchestration, dbt workflows, cloud data warehousing
