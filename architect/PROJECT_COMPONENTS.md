# Project Components & Specifications

This document defines the technical breakdown of components, modules, interfaces, and database schemas.

---

## 📦 Component Specifications

### Component A: Shared Data Models

The database models reside on Neon PostgreSQL and are decoupled across two specialized modules:

#### 1. Scraper Models (`shared/scraper_models.py`)
- **`TelegramMessage`**: Raw scraped posts from Telegram channels (`channel_name`, `message_id`, `message_text`, `date`, `media_url`, `status`, `created_at`). Keyed uniquely on `(channel_name, message_id)`.
- **`ScrapingLog`**: Per-channel day watermark audit trail (`channel_name`, `run_date`, `status`, `messages_scraped`, `messages_saved`, `messages_skipped`, `scrape_start_ts`, `scrape_end_ts`, `run_started_at`, `run_finished_at`). Keyed uniquely on `(channel_name, run_date)`.

#### 2. Annotation Platform Models (`shared/annotation_models.py`)
- **`User`**: Annotator accounts (`email`, `password_hash`, `role`, `created_at`).
- **`CleanTeleText`**: Cleaned, sentence-split Myanmar text produced by `cleaner.py` (`telegram_message_id`, `line_index`, `sentence`, `channel_name`, `source_message_id`, `created_at`).
- **`CleaningLog`**: Per-channel day watermark audit trail for cleaner (`channel_name`, `run_date`, `status`, `messages_processed`, `messages_skipped`, `sentences_generated`, `cleaning_start_ts`, `cleaning_end_ts`, `run_started_at`). Keyed uniquely on `(channel_name, run_date)`.
- **`AnnotationResult`**: Annotator submissions with JSON payload (`clean_line_id`, `user_id`, `annotation_type`, `payload_json`, `created_at`, `updated_at`). Unique on `(clean_line_id, user_id, annotation_type)`.
- **`SkippedRecord`**: Annotator skip markers per line & task (`clean_line_id`, `user_id`, `annotation_type`, `created_at`). Unique on `(clean_line_id, user_id, annotation_type)`.

---

### Component B: Telegram Scraper Bot (`services/telegram_scraper/`)

#### Modules:
1. `config.example.yaml` / `config.yaml`: Target channels, fetch limit per channel, PostgreSQL URI, cleaner settings.
2. `scraper.py`: Loads config, connects to Telethon Async client, queries messages per channel day window, and tracks channel-day watermarks in `ScrapingLog`. Supports `--dry-run`, `--lookback`, and `--force`.
3. `storage.py`: SQLAlchemy adapter for storing `TelegramMessage` records and managing `ScrapingLog` watermarks.
4. `cleaner.py`: Myanmar sentence cleaner using bigram regex split algorithm; processes `TelegramMessage` rows into `CleanTeleText` rows and logs per-channel watermarks in `CleaningLog`. Supports `--channel`, `--dry-run`, `--force`, `--lookback`, and `--dict`.
5. `cron_job.sh`: Executable bash script for server cron automation.
6. `.github/workflows/scrape_telegram.yml`: GitHub Action workflow running on cron schedule.

---

### Component C: NLP Annotation Web Platform (`services/nlp_annotation_app/`)

#### Modules & Interfaces:
1. `annotation_config.yaml`:
   Configures dynamic field definitions (Metadata fields `id`, `source`, `text`, `key_phrase`; Sub-Task 1 & 2 Polarization binary groups; Sub-Task 3 Severity binary groups).
2. `app.py`:
   - Auth routes (`/login`, `/register`, `/logout`)
   - Dashboard metrics route (`/dashboard`)
   - Annotation SPA endpoints:
     - `/api/config`: Loads active field schema and project parameters
     - `/api/state`: Returns state and field values for current line/record
     - `/api/update`: Saves live text or binary field changes to database (`payload_json`)
     - `/api/navigate` & `/api/goto`: Line jump & queue navigation
     - `/api/add` & `/api/delete`: Interactive record creation and removal
     - `/api/save`: Exports annotations to CSV, TSV, or JSON
3. `templates/`:
   - `base.html`: Modern layout navigation bar
   - `login.html`, `register.html`: Clean authentication forms
   - `dashboard.html`: Progress counters and scraped message list
   - `annotate.html`: Arloo UI with line-jump toolbar, progress counters, auto-save status, main text highlighting, binary task toggle buttons, Padauk/Noto Sans font support, and keyboard shortcuts (`Left/Right Arrow`, `Ctrl+S`).

---

## 🕒 Watermark & Idempotency Architecture Summary

Both the Scraper and Cleaner pipelines implement independent, fine-grained **per-channel day watermarks** to guarantee idempotent runs, prevent redundant processing, and allow granular backfills.

```
┌────────────────────────────────────────────────────────────────────────┐
│ Telegram Scraper (scraper.py)                                          │
│   Iterates: Target Channels × Day Windows [00:00:00 → 23:59:59 UTC]    │
│   Checks: ScrapingLog (channel_name, run_date, status='completed')     │
│   Saves: TelegramMessage rows + writes completed ScrapingLog watermark │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Myanmar Sentence Cleaner (cleaner.py)                                  │
│   Iterates: Target Channels × Day Windows (from channel's last run)    │
│   Checks: CleaningLog (channel_name, run_date, status='completed')     │
│   Splits: Myanmar sentences & writes CleanTeleText + CleaningLog       │
└────────────────────────────────────────────────────────────────────────┘
```

### Watermark Mechanism Comparison

| Feature | Scraper Watermark (`scraper.py`) | Cleaner Watermark (`cleaner.py`) |
| :--- | :--- | :--- |
| **Tracking Table** | `scraping_logs` | `cleaning_logs` |
| **Unique Constraint** | `(channel_name, run_date)` | `(channel_name, run_date)` |
| **Source Data** | Telethon API fetch | `telegram_messages` table |
| **Destination Data** | `telegram_messages` table | `clean_tele_text` table |
| **Incremental Start** | Lookback window (default: today + yesterday) | Individual latest watermark date + 1 day per channel |
| **Idempotency Guard** | Skips `(channel, run_date)` if `status == 'completed'` | Skips `(channel, run_date)` if `status == 'completed'` |
| **`--force` Overwrite** | Deletes `TelegramMessage`s for `(channel, window)`, resets log to `'running'`, re-scrapes & saves | Deletes `CleanTeleText`s for `(channel, window)`, resets log to `'running'`, re-splits & inserts |
| **Dry-Run Preview** | Supported (`--dry-run`) without DB mutations | Supported (`--dry-run`) without DB mutations |

