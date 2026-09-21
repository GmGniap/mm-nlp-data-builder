# Scraper Concurrency & Archival Architecture Specification

This document provides complete architectural specifications, operational constraints, database schemas, and orchestration designs for the Telegram Scraper and data lifecycle pipelines. It is structured to serve as an authoritative technical specification for AI agents and engineers implementing the workflows.

---

## 1. System Overview & Context

* **Target Scraper:** Telegram Scraper (`services/telegram_scraper/`), designed to be extensible to future web/API scrapers.
* **Orchestrator:** Apache Airflow (utilizing TaskFlow API with containerized or isolated environment support).
* **Storage Layer:** Neon PostgreSQL (Free Tier) for live/active transactional data.
* **Cold Storage / Data Lake:** AWS S3 storing compressed Apache Parquet files.

---

## 2. Infrastructure Constraints & Rate Limits

### 2.1 Telegram API Rate Limits
Telegram enforces strict client-level soft bans through Telethon:
* **Channel Access Ceiling:** Accessing >200 distinct channels in a rolling 24-hour window risks temporary flood waits or soft bans.
* **Batch Sizing:** Target batches of 150–200 channels max per execution window.
* **Category Partitioning:** Channels are partitioned into distinct functional categories (currently `polarization` and `news`). Each category executes as an independent batch separated by 4–6 hours.
* **Inter-Channel Throttle:** Enforce a minimum **60-second spacing** between channel scrapes (measured as `elapsed = scrape_duration; if elapsed < 60: sleep(60 - elapsed)`).
* **Chunked DB Upload Relaxation:** Every 1,000 processed messages (`t_index % 1000 == 0`), buffer-flush to PostgreSQL to persist partial progress and provide an internal pause for API relaxation.

### 2.2 Neon PostgreSQL (Free Tier Limits)
* **Compute Units:** 100 CU-hours per month (~100 hours of 1-CPU active compute).
* **Storage Limit:** 0.5 GB total storage limit.
* **Mitigation Strategy:** Active database tables retain only the most recent **30 days** of data. Data older than 30 days is automatically archived to AWS S3 in Parquet format, validated, and safely purged from PostgreSQL.

---

## 3. Concurrency & Execution Architecture

### 3.1 Telethon Concurrency & Session Safety
> [!IMPORTANT]
> Telethon's SQLite `.session` file **cannot be concurrently accessed by multiple OS processes**, as doing so raises `sqlite3.OperationalError: database is locked`.

* **Concurrency Model:** 
  * Airflow triggers category DAGs independently (staggered schedules).
  * Within each category run, concurrency across channels (up to **5 parallel channels**) is handled within an `asyncio` loop using `asyncio.Semaphore(5)` over a single shared `TelegramClient` instance utilizing `TELEGRAM_STRING_SESSION`.
  * If Airflow Dynamic Task Mapping (`expand()`) is used instead of single-task async loops, each mapped task must strictly run with an isolated in-memory `StringSession` and throttled task pool (Airflow Pool `slots=5`).
  * Can provide two different `StringSession` values to rotate and use.

### 3.2 Category-Based DAG Scheduling & Closed-Day Batch Pattern
Airflow DAGs are partitioned per category to enforce the 4–6 hour staggering window:
* `telegram_scraper_polarization_dag`: Scheduled e.g. at `0 0 * * *` (00:00 UTC).
* `telegram_scraper_news_dag`: Scheduled e.g. at `0 6 * * *` (06:00 UTC).
* DAGs dynamically parse target channels directly from `config.yaml` based on category keys.

#### Closed-Day Watermark Strategy (`--yesterday`)
* Daily batch pipelines execute with the `--yesterday` flag for both `scraper` and `cleaner`.
* **Why Closed Days:** Running a daily batch with "today" included (e.g. at 06:00 UTC) would mark the current day `completed` prematurely, leaving an 18-hour gap (06:00 to 23:59 UTC) that would be skipped on the next day's run.
* **Benefits:**
  1. **Zero Data Loss:** Day $T-1$ (`00:00:00` to `23:59:59` UTC) is 100% complete and immutable when scraped.
  2. **Optimal Telegram API Consumption:** Each channel is queried exactly once per calendar day (50% reduction in API calls compared to multi-day lookback re-scrapes).
  3. **Deterministic Watermarking:** Watermark rows in `scraping_logs` and `cleaning_logs` represent fully completed calendar days.

### 3.3 Dead Letter Queue (DLQ) & Failure Strategy
Airflow provides native scheduling and retry backoff. The scraper DLQ operates in two tiers:

1. **Tier 1: Transient Channel Retry (Airflow Native)**
   * Airflow tasks or sub-batches configure `retries=2` with `retry_delay=timedelta(minutes=10)`.
   * If a single channel encounters an unhandled exception or flood wait during execution:
     * The scraper catches the exception, logs the error, marks the channel as failed for that batch, and continues scraping the remaining channels.
2. **Tier 2: Dead Letter Logging (`scraping_error_logs`)**
   * After all retries are exhausted, or if an unrecoverable error occurs (e.g. channel banned, private channel, invalid handle), an error record is inserted into `scraping_error_logs`.
   * Manual inspection and alert notification can be triggered without failing the entire DAG run.

---

## 4. Database Schema Specifications

All tables reside in the target environment schema (`public` for `dev`, `production` for `prod`).

### 4.1 New Table: `scraping_error_logs` (DLQ Audit Table)
```sql
CREATE TABLE IF NOT EXISTS scraping_error_logs (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    channel_name VARCHAR(100) NOT NULL,
    run_date VARCHAR(10) NOT NULL,              -- YYYY-MM-DD
    error_type VARCHAR(100) NOT NULL,           -- e.g. ChannelPrivateError, FloodWaitError
    error_message TEXT NOT NULL,
    stack_trace TEXT NULL,
    retry_count INT DEFAULT 0,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_scraping_err_channel ON scraping_error_logs(channel_name);
CREATE INDEX idx_scraping_err_rundate ON scraping_error_logs(run_date);
```

### 4.2 New Table: `archival_logs` (Cold Storage Audit Table)
```sql
CREATE TABLE IF NOT EXISTS archival_logs (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,            -- 'telegram_messages' or 'clean_tele_text'
    category VARCHAR(50) NULL,
    channel_name VARCHAR(100) NULL,
    cutoff_date TIMESTAMP WITH TIME ZONE NOT NULL,
    s3_uri VARCHAR(500) NOT NULL,
    rows_archived INT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,                -- 'in_progress', 'completed', 'failed', 'deleted_from_db'
    error_message TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE NULL
);
CREATE INDEX idx_archival_logs_tbl_date ON archival_logs(table_name, cutoff_date);
```

---

## 5. Data Archival & Purge Pipeline (S3 + Parquet)

### 5.1 Critical Data Integrity Rule (Cascade Safety)
> [!CAUTION]
> In `shared/annotation_models.py`, `CleanTeleText` has cascading foreign keys:
> * `AnnotationResult` (`clean_line_id = ForeignKey('clean_tele_text.id')`, `cascade="all, delete-orphan"`)
> * `SkippedRecord` (`clean_line_id = ForeignKey('clean_tele_text.id')`, `cascade="all, delete-orphan"`)
> 
> **NEVER execute an unconstrained `DELETE FROM clean_tele_text WHERE created_at < cutoff`.** Doing so will permanently destroy human manual annotations in `annotation_results`.

#### Mandatory Deletion Rules:
1. **Raw Messages (`telegram_messages`):** Safe to delete where `date < NOW() - INTERVAL '30 days'` after successful Parquet upload.
2. **Clean Text (`clean_tele_text`):** 
   * **Option A (Safe Partial Delete):** Delete only rows where `id NOT IN (SELECT clean_line_id FROM annotation_results)` AND `created_at < NOW() - INTERVAL '30 days'`.
   * **Option B (Full Archive & Delete):** Archive `annotation_results` and `clean_tele_text` together into S3 before deleting `clean_tele_text`.

### 5.2 S3 File Hierarchy
Archival files must follow this structured path in AWS S3:
```text
s3://<S3_ARCHIVE_BUCKET_NAME>/<ENVIRONMENT>/<YEAR>/<MONTH>/<CATEGORY>/<CHANNEL_NAME>/<TABLE_NAME>-archived-<YYYY-MM-DD>.parquet
```
* **Example:** `s3://nlp-myanmar-archives/prod/2026/08/polarization/channelA/telegram_messages-archived-2026-08-01.parquet`

### 5.3 Archival Workflow Specification
1. **Execution Schedule:** Monthly or bi-weekly via dedicated Airflow DAG `telegram_archival_dag`.
2. **Date Window:** Cutoff threshold is `NOW() - INTERVAL '30 days'`. Scan excludes the most recent 2 days to prevent race conditions with running scrapers or cleaner jobs.
3. **Extraction & Compression:**
   * Query records in chunks (e.g. 5,000 rows).
   * Convert to PyArrow Table (`pyarrow.Table.from_pylist(...)`).
   * Write with `snappy` or `zstd` compression to temporary local Parquet buffer.
4. **S3 Upload:**
   * Stream/Upload Parquet file to S3 via `boto3.client('s3')`.
   * Verify S3 object exists and byte size matches local file.
5. **Validation Check:**
   * Read Parquet metadata or load record count from S3 Parquet file.
   * Compare `s3_record_count == db_queried_count`.
6. **Idempotent Purge:**
   * Only upon verified Parquet count equality: execute transactional deletion from PostgreSQL for the archived row IDs.
   * Record outcome and counts in `archival_logs`.

---

## 6. Configuration Specification (`config.yaml`)

The `config.yaml` file is structured to support category-based batches, rate limiting, and archival configuration:

```yaml
# Telegram Scraping & Archival Configuration
environment: dev  # 'dev' (uses 'public' schema) or 'prod' (uses 'production' schema)

telegram:
  api_id: "36219741478920"
  api_hash: "cffff71bdeddd35887e6766992b7773582fa8d0ef4"
  session_name: "telegram_session"
  # StringSession is preferred in production to allow worker mobility
  string_session: ""

scraping:
  max_parallel_channels: 5
  inter_channel_delay_seconds: 60
  db_upload_chunk_size: 1000
  limit_per_channel: 200
  lookback_days: 2

  # Category definitions and cron staggered offsets
  categories:
    polarization:
      schedule_cron: "0 0 * * *"       # Runs at 00:00 UTC
      channels:
        - "@channel_name"
    news:
      schedule_cron: "0 6 * * *"       # Runs at 06:00 UTC (6h offset)
      channels:
        - "@channel_name"

# Archival to AWS S3 & Data Retention
archival:
  retention_days: 30
  scan_safety_offset_days: 2
  batch_chunk_size: 5000
  compression: "snappy"
  s3:
    bucket_name: "myanmar-nlp-data-archive"
    region: "ap-southeast-1"

# Database Connection (Neon PostgreSQL)
postgresql:
  url: "postgresql://neondb_owner:password@ep-host.aws.neon.tech/neondb?sslmode=require"
```

---

## 7. Implementation Checklist for AI Agent

- [ ] **1. Dependencies & Models:**
  - Add `pyarrow>=14.0.0` and `boto3>=1.34.0` to `pyproject.toml` and `services/telegram_scraper/requirements.txt`.
  - Add SQLAlchemy models `ScrapingErrorLog` and `ArchivalLog` into `shared/scraper_models.py` and `init_scraper_db`.
- [ ] **2. Core Scraper Updates (`scraper.py` & `storage.py`):**
  - Update `load_config()` to handle category-based structures and limits.
  - Implement `asyncio.Semaphore(max_parallel_channels)` in `scraper.py` for concurrent channel extraction.
  - Add inter-channel rate limiting delay (`inter_channel_delay_seconds`).
  - Add chunked flushing every `db_upload_chunk_size` messages.
  - Add DLQ helper `log_scraping_error(...)` in `storage.py` writing to `scraping_error_logs`.
- [ ] **3. Airflow DAGs (`airflow_examples/` or dedicated DAG folder):**
  - Create category-specific DAG generator or distinct DAGs (`telegram_scraper_polarization_dag.py`, `telegram_scraper_news_dag.py`).
  - Configure task retries (`retries=2, retry_delay=timedelta(minutes=10)`).
  - Implement `telegram_archival_dag.py` running on a monthly schedule.
- [ ] **4. Archival Module (`services/telegram_scraper/archival.py`):**
  - Implement query for messages older than 30 days (excluding last 2 days).
  - Implement PyArrow conversion and Parquet streaming to AWS S3.
  - Implement verification step comparing S3 row counts to DB row counts.
  - Implement safe DB deletion (with foreign key protection on `clean_tele_text`).
  - Write run details to `archival_logs`.
- [ ] **5. Unit & Integration Tests:**
  - Mock S3 upload with `moto` or mock client to test Parquet generation and row-count verification.
  - Test `clean_tele_text` deletion logic to verify annotated rows are never deleted.
  - Test `asyncio.Semaphore` channel batching under mocked Telethon responses.