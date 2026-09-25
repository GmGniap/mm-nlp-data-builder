# Sub-Agent Instructions & Operational Guidelines

This document provides explicit instructions, responsibilities, and operational guidelines for Agent 2 (Python & Data Engineer Agent) and Agent 3 (Full-Stack & Annotation Platform Agent).

---

## 📋 Prerequisites

Before executing tasks, both agents must ensure:
- Virtual environment is active: `source .venv/bin/activate`.
- Database credentials use `<NEON_DATABASE_URL>`.
- Sensitive API keys use environment variables: `<TELEGRAM_API_ID>`, `<TELEGRAM_API_HASH>`, `<TELEGRAM_STRING_SESSION>`.
- Cold storage destination uses `<S3_ARCHIVE_BUCKET>`.
- No real credentials, private IPs, or production bucket URLs are committed to source files.

---

## 🐍 Agent 2: Python & Data Engineering Agent (Scraper & NLP Cleaner)

### Primary Mission
Build, orchestrate, and maintain a robust, config-driven, rate-limited Telegram data acquisition pipeline, a decoupled Myanmar NLP sentence cleaning engine, and automated S3 Parquet archival.

### Database Architecture
All scraper, cleaner, and archival tables reside on **Neon PostgreSQL** within the environment schema (`public` for dev, `production` for prod):

| Table | Role | Written By | Read By |
|---|---|---|---|
| `telegram_messages` | Raw scraped posts from Telegram | `scraper.py` | `cleaner.py`, `archival.py` |
| `scraping_logs` | Scraper per-channel closed-day watermarks | `scraper.py` | `scraper.py`, Airflow |
| `scraping_error_logs` | Scraper Dead Letter Queue (DLQ) | `scraper.py` | Monitoring, `--retry-dlq` |
| `clean_tele_text` | Cleaned, sentence-split Myanmar lines | `cleaner.py` | Flask annotation app |
| `clean_tele_extra_info` | News metadata (`headline`, `clean_info_date`, `original_short_note`, `url_lists`) | `cleaner.py` | Flask annotation app, NER pipelines |
| `cleaning_logs` | Cleaner per-channel closed-day watermarks | `cleaner.py` | `cleaner.py`, Airflow |
| `cleaning_error_logs` | Cleaner Dead Letter Queue (DLQ) | `cleaner.py` | Monitoring, `--retry-dlq` |
| `archival_logs` | Cold storage Parquet audit trail | `archival.py` | Data Engineering |

### Workspace Scope
`services/telegram_scraper/`, `shared/scraper_models.py`, `shared/annotation_models.py`, and `services/telegram_scraper/airflow_dags/`.

### Core Directives & Checklist

1. **Config-Driven Scraper Concurrency (`scraper.py`)**:
   - Parse `config.yaml` to read category channel lists (`polarization`, `news`).
   - Use `asyncio.Semaphore(5)` for channel extraction concurrency over Telethon `StringSession`.
   - Throttle inter-channel requests with a minimum 60-second delay.
   - Enforce closed-day processing (`--yesterday`) querying $T-1$ (`00:00:00` to `23:59:59` UTC).
   - Log unrecoverable channel failures to `scraping_error_logs`.
   - Support `--retry-dlq` to re-process failed channels.

2. **Decoupled Sentence Cleaner & Staging (`cleaner.py`)**:
   - **News Transformation:** Extract Line 1 as `headline` (dual storage in `clean_tele_extra_info` and `clean_tele_text` with `line_index = 0`), normalize Line 2 into strict `YYYY-MM-DD` date (`clean_info_date`) and store raw text in `original_short_note`, extract web URLs into `url_lists`.
   - **Polarization Transformation:** Intra-channel daily deduplication (SHA-256), emoji and noise stripping, English-only sentence detection, short sentence filter (< 8 syllables), and unmonitored channel discovery (`[DISCOVERY]` logging).
   - **Staging Mode (`--stage-dir`):** Stage channel records into `data/clean_staging/<category>/...jsonl` without persistent PostgreSQL write locks.
   - **Barrier Bulk Ingestion (`--upload-staged`):** Ingest staged files in a single transaction via `bulk_insert_mappings` (1,000 items/batch).
   - **Cascade Safety:** On `--force` re-cleaning, exclude rows already present in `annotation_results` from deletion.

3. **Cold Storage Archival & Safe Purge (`archival.py`)**:
   - Extract records older than 30 days (excluding recent 2 days).
   - Convert to PyArrow tables and stream compressed Parquet to AWS S3 (`s3://<S3_ARCHIVE_BUCKET>/...`).
   - Validate row counts between S3 and database before executing transactional deletion.
   - Never delete `clean_tele_text` rows that have active human annotations in `annotation_results`.

4. **Airflow Orchestration**:
   - Maintain separate scheduled DAGs: `telegram_scraper_polarization_dag.py` (`00:00 UTC`), `telegram_scraper_news_dag.py` (`06:00 UTC`), `telegram_cleaning_dag.py` (`08:00 UTC`), and `telegram_archival_dag.py` (monthly).

---

## 🎨 Agent 3: Full-Stack Developer Agent (Annotation Platform & Microservices)

### Primary Mission
Build, enhance, and maintain an intuitive, high-performance Flask web platform for annotators to review cleaned sentence lines, perform NER / polarization / severity tagging, export research datasets, and record browser speech audio.

### Workspace Scope
`services/nlp_annotation_app/`, `services/recording_app/`, and `services/` (Gateway / Core App).

### Core Directives & Checklist

1. **Flask Application Architecture**:
   - Use application factory pattern (`create_app`) with `ProxyFix` middleware.
   - Maintain modular blueprints for Authentication, Dashboard, Annotation Feature, and Recording Service.
   - Connect directly to Neon PostgreSQL using `<NEON_DATABASE_URL>`.

2. **Annotation Interface Data Flow**:
   - Annotators label sentences from `clean_tele_text`.
   - Support category filtering (e.g. `news` vs `polarization`).
   - Query `clean_tele_extra_info` by `(channel_name, message_id)` to display context headlines, standardized dates, and reference URLs.
   - Store annotations in `AnnotationResult` (payload JSON) and skips in `SkippedRecord`.
   - Maintain dynamic resume navigation (`index = -1`) loading the first unannotated record for the active user.

3. **Speech Recording Microservice (`services/recording_app/`)**:
   - Stream PCM audio chunks from browser `AudioWorklet` to disk-backed bounded temporary buffers.
   - Atomically finalize WAV audio and JSON metadata.
   - Support standalone access or navigation from the main platform.
