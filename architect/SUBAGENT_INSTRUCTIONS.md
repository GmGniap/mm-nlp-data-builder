# Sub-Agent Instructions & Operational Guidelines

This document provides explicit instructions, responsibilities, and operational prompts for Agent 2 (Python Developer Agent) and Agent 3 (Full-Stack Developer Agent).

---

## Python Developer Agent (Telegram Scraper)

### Primary Mission
Build a robust, config-driven, automated Telegram channel message scraper that stores structured text messages in the shared SQLite database, and run a Myanmar sentence-cleaning pipeline that uploads split sentences to Neon PostgreSQL.

### Architecture — Two-Database Design
| Database | Role | Who writes | Who reads |
|---|---|---|---|
| **PostgreSQL** | Cleaned `CleanTeleText` sentence rows | `cleaner.py` | Flask annotation app |

### Workspace Scope
`services/telegram_scraper/` and `.github/workflows/scrape_telegram.yml`

### Checklist & Guidelines
1. **Config-Driven Architecture**:
   - Use `PyYAML` to read configuration parameters from `config.yaml` (fallback to `config.example.yaml`).
   - Use environment variables (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`) for sensitive API credentials.
   - Store the Neon PostgreSQL connection string as `NEON_DATABASE_URL` env var (or `.env` file); also document it under the `postgresql.url` config key.
2. **Scraper Implementation (`scraper.py`)**:
   - Use `telethon` or `pyrogram` for fetching channel posts asynchronously.
   - Implement deduplication using unique constraint on `(channel_name, message_id)`.
   - Record scraping metadata (timestamp, channel name, text, media links if any).
3. **Database Integration (`storage.py`)**:
   - Save extracted messages directly into the `TelegramMessage` database model defined in `shared/models.py`.
   - Set default status to `"pending"`.
4. **Cleaning Pipeline (`cleaner.py`)**:
   - After scraping, run `cleaner.py` to split each `TelegramMessage.message_text` into individual Myanmar sentences using the built-in bigram splitter (ported from `my-linebreak.pl` by Dr.Ye Kyaw Thu).
   - Upload resulting `CleanTeleText` rows (one per sentence) to Neon PostgreSQL.
   - Support `--dry-run` (print splits without writing) and `--force` (re-process already-cleaned messages).
   - Support an optional custom bigram dict file via `--dict PATH` or `cleaner.dict_path` in config.
5. **Automation Setup**:
   - Ensure `cron_job.sh` is executable (`chmod +x cron_job.sh`) and contains environment activation logic.
   - Verify `.github/workflows/scrape_telegram.yml` defines secret environment variable mapping for GitHub Actions.
6. **Testing**:
   - Implement dry-run mode (`python scraper.py --dry-run`) to test fetching without committing to database.
   - Implement dry-run mode (`python cleaner.py --dry-run`) to verify sentence splitting without PostgreSQL writes.

---

## Full-Stack Developer Agent (NLP Annotation Platform)

### Primary Mission
Build an intuitive, responsive Flask web application for annotators to review individual **cleaned sentence lines** from Telegram messages, apply NLP tags, and export structured datasets.

### Data Flow (Important)
Raw Telegram messages are stored in `TelegramMessage`. A separate cleaning pipeline splits each message into individual sentences and stores them in `CleanTeleText` (one row per sentence). **The annotation interface must operate on `CleanTeleText` rows**, not raw `TelegramMessage` blobs. The `CleanTeleText.telegram_message_id` foreign key links each sentence back to its source message.

### Workspace Scope
`services/nlp_annotation_app/`

### Checklist & Guidelines
1. **Flask Application Architecture (`app.py`)**:
   - Maintain modular blueprints or routing structure for Auth, Dashboard, and Annotation.
   - Support `Flask-Login` authentication flow (register, login, logout, password hashing).
2. **Database Models (`shared/annotation_models.py`)**:
   - Import and use `User`, `CleanTeleText`, `CleaningLog`, `AnnotationResult`, and `SkippedRecord` from `shared.annotation_models`.
   - The annotation app uses Neon PostgreSQL directly via `NEON_DATABASE_URL`.
3. **Annotation Data Source — `CleanTeleText`**:
   - All annotation-facing API routes (`/api/state`, `/api/submit`, `/api/skip`, `/api/save`, `/api/config`) must query `CleanTeleText` for the list of items to annotate.
   - `AnnotationResult` stores submitted annotations as a flexible JSON blob (`payload_json`) per user, per sentence (`clean_line_id`), and per annotation type.
   - `SkippedRecord` tracks skipped sentences per user and per annotation type.
   - `CleanTeleText` contains denormalized columns (`channel_name`, `source_message_id`) to build Telegram deep-links without needing to query the scraper's database.
   - **Last State Management**: `/api/state` should support `index=-1` (as default) to dynamically query the database for the first `CleanTeleText` row not present in `AnnotationResult` or `SkippedRecord` for the current user. This enables independent progress tracking across multiple users.
4. **Dashboard (`dashboard.html`)**:
   - Statistics should reflect `CleanTeleText` row counts (total sentences, pending, annotated, skipped).
   - Display the latest `CleaningLog` watermark timestamp to show when the database was last updated by the scraper pipeline.
5. **Web User Interface (`templates/`)**:
   - Use modern styling (Tailwind CSS via CDN) in `base.html`.
   - Build `dashboard.html` to visualize queue statistics.
   - Build `annotate.html` (Arloo UI) offering interactive text selection, binary task toggle buttons, and dynamic field configuration via `annotation_config.yaml`.
   - The UI defaults `IDX` to `-1` to trigger the backend's dynamic resume logic on load.
6. **API & Data Export**:
   - Implement `/api/submit` and `/api/skip` POST routes to submit/skip annotations and update `AnnotationResult` / `SkippedRecord` tables.
   - Implement `/api/save` GET route allowing users to download labeled datasets as JSON / CSV / TSV.
7. **Testing**:
   - Test application startup locally on port 5000 (`python app.py`).
