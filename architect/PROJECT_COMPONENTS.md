# Project Components & Specifications

This document defines the technical breakdown of components, modules, interfaces, and database schemas.

---

## 📦 Component Specifications

### Component A: Shared Data Core (`shared/models.py`)
Provides unified database entity definitions for both Scraper (Agent 2) and Annotation Platform (Agent 3).

#### Data Entities:
```python
# 1. User Entity
User:
  - id: Integer (Primary Key)
  - email: String(120), Unique, Not Null
  - password_hash: String(256), Not Null
  - role: String(20), Default "annotator"
  - created_at: DateTime

# 2. TelegramMessage Entity
TelegramMessage:
  - id: Integer (Primary Key)
  - channel_name: String(100), Not Null
  - message_id: BigInteger, Not Null
  - message_text: Text, Not Null
  - date: DateTime, Nullable
  - media_url: String(500), Nullable
  - status: String(20), Default "pending"
  - created_at: DateTime

# 3. AnnotationTag Entity (Config-Driven & Payload Supported)
AnnotationTag:
  - id: Integer (Primary Key)
  - message_id: Integer (Foreign Key -> TelegramMessage.id)
  - user_id: Integer (Foreign Key -> User.id)
  - tag_type: String(50), Nullable # e.g., "NER", "POLARIZATION", "SEVERITY"
  - label: String(100), Nullable   # e.g., "PER", "political", "stereotype"
  - start_offset: Integer, Nullable
  - end_offset: Integer, Nullable
  - text_snippet: Text, Nullable
  - payload_json: Text, Nullable   # JSON payload storing all config field values
  - created_at: DateTime
```

---

### Component B: Telegram Scraper Bot (`services/telegram_scraper/`)

#### Modules:
1. `config.example.yaml` / `config.yaml`: Target channels, fetch limit per channel, storage URI.
2. `scraper.py`: Loads config, connects to Telethon Async client (with fallback dry-run / stub mode).
3. `storage.py`: SQLAlchemy adapter for batch storing `TelegramMessage` records.
4. `cron_job.sh`: Executable bash script for server cron automation.
5. `.github/workflows/scrape_telegram.yml`: GitHub Action workflow running on cron schedule.

---

### Component C: NLP Annotation Web Platform — Arloo Integration (`services/nlp_annotation_app/`)

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
