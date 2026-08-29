# Project Overview & Architecture Decisions

This document details the overall architectural vision, infrastructure choices, data pipeline, and system integration strategies for the Telegram Scraping and NLP Annotation Platform.

---

## 📍 System Architecture & Pipeline Flow

The project is structured as an end-to-end data acquisition and NLP labeling ecosystem.

```
+--------------------------+       +----------------------------+       +-------------------------------+
|  Telegram Public Channels| ----> | Agent 2: Telegram Scraper  | ----> | Shared SQLite/PostgreSQL DB   |
+--------------------------+       | (Config-driven + Cron/GHA) |       +-------------------------------+
                                   +----------------------------+                       |
                                                                                        v
                                                                        +-------------------------------+
                                                                        | Agent 3: NLP Annotation Web UI|
                                                                        | (Flask + Tailwind CSS + Auth) |
                                                                        +-------------------------------+
```

---

## 🛠️ Key Infrastructure Decisions

### 1. Database Architecture
- **Development Storage**: SQLite (`shared_data.db`) located in standard instance directory for rapid setup without external service dependencies.
- **Production Storage**: PostgreSQL supported via SQLAlchemy database URI (`DATABASE_URL`).
- **Data Models**:
  - `User`: Handles platform authentication (Admin, Annotator).
  - `TelegramMessage`: Stores channel metadata, message ID, timestamp, raw content, and processing status (`pending`, `annotated`, `skipped`).
  - `AnnotationTag`: Stores labeled entities, text classifications, sentiment scores, annotator ID, and submission timestamp.

### 2. Scraping Infrastructure (Agent 2)
- **Library**: `Telethon` (or `Pyrogram`) async Python client.
- **Configuration**: `config.yaml` defines target channels, fetch interval, batch size, keywords filter, and API keys.
- **Automation**:
  - **Cron Execution**: `services/telegram_scraper/cron_job.sh` runs periodically on local/server environments.
  - **Cloud Automation**: `.github/workflows/scrape_telegram.yml` runs scheduled GitHub Actions workflow with secret injection (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`).

### 3. Annotation Platform Infrastructure (Agent 3)
- **Backend Framework**: Python Flask.
- **Frontend Stack**: Jinja2 HTML templates + Tailwind CSS (via CDN) + JavaScript interactive token-tagger.
- **Authentication**: `Flask-Login` with hashed passwords (`Werkzeug.security`).
- **Exporting**: Supports JSON/CSV export of labeled datasets for downstream NLP training (spaCy, HuggingFace Transformers).

---

## 🎯 System Milestones

1. **Phase 1: Workspace & Schema Initialization** (Current)
   - Establish agent workspaces (`architect/`, `services/telegram_scraper/`, `services/nlp_annotation_app/`).
   - Define shared database schemas.
2. **Phase 2: Scraper Bot Development**
   - Config parsing, Telethon client connection, channel iteration, deduplication, and database insertion.
   - Automation script testing (Cron + GitHub Actions).
3. **Phase 3: Web Annotation UI Development**
   - Flask authentication (login/register).
   - Dashboard showing scrape metrics and queue state.
   - Interactive annotation page for token highlight / NER tagging & classification.
4. **Phase 4: Pipeline Integration & Exporting**
   - End-to-end test from Telegram scraping to web labeling to dataset export.
