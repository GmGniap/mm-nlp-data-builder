# Agent 2 Workspace: Telegram Scraping Service

This service provides an automated, config-driven Telegram channel scraper using Telethon and SQLAlchemy, managed via `uv` and Python 3.13.

---

## 📐 Architecture — Two-Database Design

```
scraper.py ──writes──► Postgresql (main_data)
                              │
                        cleaner.py reads raw TelegramMessage rows
                              │
                         splits into Myanmar sentences
                              │
                              └──writes──► PostgreSQL (clean_data)
                                                  │
                                         Flask annotation app reads
```

Separating the write path eliminates SQLite lock contention between the scraper and the Flask app.

---

## 📁 Directory Layout

```
.
├── config.example.yaml     # Template configuration file
├── login_telegram.py       # One-time interactive terminal login script
├── scraper.py              # Main scraping script with Telethon & fallback stub mode
├── cleaner.py              # Myanmar sentence splitter & Neon PostgreSQL uploader
├── storage.py              # Database persistence layer using shared database models
├── cron_job.sh             # Executable shell script for system cron jobs
└── requirements.txt        # Python dependencies for scraper
```

---

## ⚙️ How to Fix "Telethon Client Not Authorized"

Telegram requires a **one-time interactive login** to authorize your account and generate a `.session` file (e.g. `telegram_research.session`).

### Step 1: Run the Interactive Login Script
In your terminal, execute:

```bash
uv run python services/telegram_scraper/login_telegram.py
```

### Step 2: Complete the Prompts
1. Enter your phone number with country code (e.g., `+619123456789`).
2. Enter the login code Telegram sends to your Telegram app / SMS.
3. If you have 2-Factor Authentication enabled, enter your password.

Once completed, a `.session` file will be created in `services/telegram_scraper/`. Subsequent scraper runs will automatically reuse this session file without asking for login credentials!

---

## 🚀 Running the Scraper

### Normal Scraping Run
```bash
uv run python services/telegram_scraper/scraper.py
```

### Dry Run (Test without database writes)
```bash
uv run python services/telegram_scraper/scraper.py --dry-run
```

---

## 🧹 Running the Cleaner Pipeline

The cleaner reads raw `TelegramMessage` rows from SQLite, splits the Myanmar text into individual sentences, and uploads `CleanTeleText` rows to your Neon PostgreSQL server.

### Prerequisites

Set your Neon connection string (preferred via environment variable):
```bash
export NEON_DATABASE_URL="postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require"
```
Or add it to your `.env` file, or fill in `postgresql.url` in `config.yaml`.

### Normal Run
```bash
uv run python services/telegram_scraper/cleaner.py
```

### Dry Run (print sentences, no PostgreSQL writes)
```bash
uv run python services/telegram_scraper/cleaner.py --dry-run
```

### Force Re-process (overwrite existing CleanTeleText rows)
```bash
uv run python services/telegram_scraper/cleaner.py --force
```

### Custom Bigram Dictionary
The cleaner uses a built-in Myanmar bigram list by default. To use a custom
`syllable<space>potema` dict file (one entry per line, same format as `1syl.potma.dict`
used by `my-linebreak.pl`):
```bash
uv run python services/telegram_scraper/cleaner.py --dict /path/to/my.dict
```

---

## ⏰ Automation

### 1. Cron Job
Add to crontab (`crontab -e`):
```cron
0 */6 * * * /path/to/test_ai_project/services/telegram_scraper/cron_job.sh
```

### 2. GitHub Actions
The workflow in `.github/workflows/scrape_telegram.yml` triggers every 6 hours automatically once GitHub Secrets (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `NEON_DATABASE_URL`) are configured.
