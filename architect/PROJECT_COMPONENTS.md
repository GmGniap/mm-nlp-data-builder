# Project Components & Technical Specifications

This document defines the comprehensive technical breakdown of system components, modules, interfaces, and database schemas across the Telegram Scraping, Myanmar NLP Cleaning, and Annotation Platform.

---

## 📋 Prerequisites

- **Python Environment:** Python 3.10+ activated within `.venv`.
- **Database Connection:** Neon PostgreSQL connection string (`postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>/<DB_NAME>?sslmode=require`).
- **Cloud Storage:** S3 bucket configured for cold storage Parquet archival (`s3://<S3_ARCHIVE_BUCKET>/...`).
- **Configuration File:** `config.yaml` structured with target categories, channel handles, and limits.

---

## 📦 Component Specifications

### Component A: Shared Data Models (`shared/`)

Database models reside on Neon PostgreSQL and are decoupled across two specialized modules to enforce service isolation:

#### 1. Scraper Models (`shared/scraper_models.py`)
- **`TelegramMessage`**: Raw scraped posts from Telegram channels (`id`, `channel_name`, `category`, `message_id`, `message_text`, `date`, `media_url`, `status`, `created_at`). Unique on `(channel_name, message_id)`.
- **`ScrapingLog`**: Per-channel day watermark audit trail (`id`, `channel_name`, `category`, `run_date`, `status`, `messages_scraped`, `messages_saved`, `messages_skipped`, `scrape_start_ts`, `scrape_end_ts`, `run_started_at`, `run_finished_at`). Unique on `(channel_name, run_date)`.
- **`ScrapingErrorLog`**: Scraper Dead Letter Queue (DLQ) tracking channel scrape failures (`id`, `channel_name`, `category`, `run_date`, `error_type`, `error_message`, `stack_trace`, `retry_count`, `resolved`, `created_at`).
- **`ArchivalLog`**: Audit trail for S3 Parquet cold storage jobs and safe DB deletions (`id`, `table_name`, `category`, `channel_name`, `cutoff_date`, `s3_uri`, `rows_archived`, `file_size_bytes`, `status`, `error_message`, `created_at`, `finished_at`).

#### 2. Annotation Platform Models (`shared/annotation_models.py`)
- **`User`**: Annotator accounts (`id`, `email`, `password_hash`, `role`, `created_at`).
- **`CleanTeleText`**: Cleaned, sentence-split Myanmar text produced by `cleaner.py` (`id`, `telegram_message_id`, `line_index`, `sentence`, `channel_name`, `category`, `source_message_id`, `created_at`).
- **`CleanTeleExtraInfo`**: News metadata table (`id`, `channel_name`, `category`, `message_id`, `headline`, `clean_info_date`, `original_short_note`, `url_lists`, `created_at`). Independent table without foreign key constraints to support direct NER tagging.
  - `clean_info_date`: Strictly formatted ISO date `YYYY-MM-DD` (e.g. `'2026-09-20'`).
  - `original_short_note`: Complete verbatim Line 2 text (e.g. `'မကွေး၊ စက်တင်ဘာ ၂၀ ရက်'`).
  - `url_lists`: JSON array of extracted external web and video URLs.
- **`CleaningLog`**: Per-channel day watermark audit trail for cleaner (`id`, `channel_name`, `category`, `run_date`, `status`, `messages_processed`, `messages_skipped`, `sentences_generated`, `cleaning_start_ts`, `cleaning_end_ts`, `run_started_at`). Unique on `(channel_name, run_date)`.
- **`CleaningErrorLog`**: Cleaner Dead Letter Queue (DLQ) tracking transformation exceptions (`id`, `channel_name`, `category`, `run_date`, `telegram_message_id`, `source_message_id`, `raw_text`, `error_type`, `error_message`, `stack_trace`, `retry_count`, `resolved`, `created_at`, `resolved_at`).
- **`AnnotationResult`**: Annotator submissions with JSON payload (`id`, `clean_line_id`, `user_id`, `annotation_type`, `payload_json`, `created_at`, `updated_at`). Unique on `(clean_line_id, user_id, annotation_type)`.
- **`SkippedRecord`**: Annotator skip markers per line & task (`id`, `clean_line_id`, `user_id`, `annotation_type`, `created_at`). Unique on `(clean_line_id, user_id, annotation_type)`.

---

### Component B: Telegram Scraper & Data Pipeline (`services/telegram_scraper/`)

#### Key Modules:
1. `config.yaml` / `config.example.yaml`: Category definitions (`polarization`, `news`), target channels, rate limits, PostgreSQL URI, and S3 archival settings.
2. `scraper.py`:
   - Connects to Telethon via persistent `StringSession`.
   - Category-based batching (`--category`) with `asyncio.Semaphore(5)` for channel-level concurrency.
   - Inter-channel rate limiting throttle (60s minimum spacing).
   - Closed-day batching (`--yesterday`) querying $T-1$ (`00:00:00` to `23:59:59` UTC).
   - DLQ trapping with `--retry-dlq` CLI argument.
3. `storage.py`: SQLAlchemy database adapter for message deduplication, watermark management, and DLQ logging.
4. `cleaner.py`:
   - Decoupled category transformation pipelines (`news` vs `polarization`).
   - **News Transformation:** Line 1 headline extraction (dual-stored in `clean_tele_extra_info` and `clean_tele_text` with `line_index = 0`), Line 2 dateline normalization (`clean_info_date` in `YYYY-MM-DD`, `original_short_note`), external URL extraction (`url_lists`).
   - **Polarization Transformation:** Intra-channel daily deduplication (SHA-256), emoji and noise stripping, English-only sentence removal, short-sentence dropping (< 8 syllables), and unmonitored Telegram channel discovery (`[DISCOVERY]` logging).
   - **File Staging Engine (`--stage-dir`):** Writes intermediate records to `data/clean_staging/<category>/...jsonl` without persistent DB write locks.
   - **Barrier Bulk Upload (`--upload-staged`):** Single-transaction chunked ingestion (`bulk_insert_mappings`) protecting Neon compute units.
   - **DLQ & Safe Reprocessing (`--retry-dlq`):** Traps failures in `cleaning_error_logs`.
   - **Cascade Safety:** `--force` re-processing deletes unannotated sentences while strictly preserving lines present in `annotation_results`.
5. `archival.py`:
   - Scans records older than 30 days (excluding recent 2 days).
   - Streams compressed Apache Parquet files directly to AWS S3.
   - Compares record counts between S3 and DB before triggering transactional deletion.
   - Excludes human-annotated sentences from deletion.
6. `migrate_cleaner_category.py`: CLI migration utility (`--env dev|prod`, `--dry-run`) applying DDL and backfilling categories.
7. **Airflow Orchestration DAGs:**
   - `telegram_scraper_polarization_dag.py`: Scheduled at `00:00 UTC`.
   - `telegram_scraper_news_dag.py`: Scheduled at `06:00 UTC`.
   - `telegram_cleaning_dag.py`: Scheduled at `08:00 UTC` with TaskGroups for staging and bulk uploads.
   - `telegram_archival_dag.py`: Scheduled monthly for S3 cold storage.

---

### Component C: Core Web Platform & Gateway (`services/`)

The core platform manages application initialization, authentication, global UI layouts, and microservice orchestration:
1. `app.py`: Main Flask application factory (`create_app`) and entrypoint. Includes `ProxyFix` middleware for reverse-proxy compatibility.
2. `extensions.py`: Central SQLAlchemy `db` and Flask-Login `login_manager` instances.
3. `models.py`: Core `User` model, password hashing, and user loader.
4. `services_manager.py`: Local development manager for auxiliary microservices.
5. `templates/`: Central shared templates (`base.html`, `dashboard.html`, `login.html`, `register.html`).

---

### Component D: NLP Annotation Feature Module (`services/nlp_annotation_app/`)

Self-contained feature module plugged into the core platform via Flask Blueprint:
1. `annotation_config.yaml`: Dynamic field definitions (Metadata fields, Sub-Task 1 & 2 Polarization binary groups, Sub-Task 3 Severity binary groups).
2. `routes.py`:
   - Blueprint (`annotation_bp`) mounted at `/annotation/`
   - REST API endpoints (`/api/config`, `/api/state`, `/api/submit`, `/api/skip`, `/api/update`, `/api/navigate`, `/api/save`).
   - Dynamic resume capability (`index = -1` loads first unannotated record for current user).
3. `models.py`: Feature models (`CleanTeleText`, `CleanTeleExtraInfo`, `CleaningLog`, `AnnotationResult`, `SkippedRecord`).
4. `templates/annotate.html`: Interactive token-tagging and classification interface.

---

### Component E: Speech Recording Microservice (`services/recording_app/`)

Browser-based audio collection microservice running on port 5001 (or via reverse proxy):
1. `app.py`: Signed API, prompt streaming, and cleanup CLI.
2. `storage.py`: Bounded chunks and atomic WAV finalization.
3. `templates/recorder.html` & `static/`: AudioWorklet and state machine.

---

## 🕒 Watermark, Idempotency & Error Handling Matrix

Both the Scraper and Cleaner pipelines implement independent, fine-grained **per-channel day watermarks** to guarantee idempotent runs, prevent redundant processing, and allow granular backfills.

```
┌────────────────────────────────────────────────────────────────────────┐
│ Telegram Scraper (scraper.py)                                          │
│   Iterates: Target Channels × Closed Day (Yesterday: 00:00 → 23:59 UTC)│
│   Checks: ScrapingLog (channel_name, run_date, status='completed')     │
│   Traps Errors: scraping_error_logs (DLQ)                              │
│   Saves: TelegramMessage rows + completed ScrapingLog watermark        │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Myanmar Sentence Cleaner (cleaner.py)                                  │
│   Iterates: Target Channels × Category (news / polarization)           │
│   Stages: Local JSONL files in data/clean_staging/<category>/          │
│   Traps Errors: cleaning_error_logs (DLQ)                              │
│   Barrier Upload: Single transaction bulk insert into PostgreSQL       │
│   Saves: CleanTeleText + CleanTeleExtraInfo + CleaningLog watermark    │
└────────────────────────────────────────────────────────────────────────┘
```

### Watermark Mechanism Comparison

| Feature | Scraper Watermark (`scraper.py`) | Cleaner Watermark (`cleaner.py`) |
| :--- | :--- | :--- |
| **Tracking Table** | `scraping_logs` | `cleaning_logs` |
| **Unique Constraint** | `(channel_name, run_date)` | `(channel_name, run_date)` |
| **Source Data** | Telethon API fetch | `telegram_messages` table |
| **Destination Data** | `telegram_messages` table | `clean_tele_text` & `clean_tele_extra_info` |
| **DLQ Error Table** | `scraping_error_logs` | `cleaning_error_logs` |
| **Incremental Scope** | Closed day (`--yesterday`) | Staged channel JSONL files for closed day |
| **Idempotency Guard** | Skips `(channel, run_date)` if `status == 'completed'` | Skips `(channel, run_date)` if `status == 'completed'` |
| **`--force` Overwrite** | Deletes `TelegramMessage`s for `(channel, window)`, resets log to `'running'`, re-scrapes & saves | Deletes **unannotated** `CleanTeleText`s for `(channel, window)`, preserves human annotations, re-cleans & inserts |
| **Dry-Run Preview** | Supported (`--dry-run`) without DB mutations | Supported (`--dry-run`) without DB mutations |
| **Retry Failed DLQ** | Supported (`--retry-dlq`) | Supported (`--retry-dlq`) |
