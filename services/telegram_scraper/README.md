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

## 🐳 Docker Packaging & Airflow Integration

The scraper and cleaner are fully containerized and ready to run as an Airflow task (`DockerOperator`, `KubernetesPodOperator`, or cloud task runner).

### 1. Build and Push to CNCF Registry
Use the automated deployment script [`deploy_docker.sh`](file:///Users/thetpaing/Documents/Coding/test_ai_project/deploy_docker.sh):
```bash
# Build and push with defaults (v0.1 to your registry)
./deploy_docker.sh

# Dynamic options (custom tag, latest alias, custom image name or dockerfile)
./deploy_docker.sh -t v0.2 --latest
./deploy_docker.sh -i custom-scraper -t 1.0.0 -f services/telegram_scraper/Dockerfile
```

Or build manually via Docker:
```bash
docker build --platform linux/arm64 -f services/telegram_scraper/Dockerfile -t registry.example.com:5005/telegram-scraper:v0.1 .
docker push registry.example.com:5005/telegram-scraper:v0.1
```

### 2. Test Container Locally

**Dry-run Scraper:**
```bash
docker run --rm \
  -e TELEGRAM_API_ID="your_api_id" \
  -e TELEGRAM_API_HASH="your_api_hash" \
  -e TELEGRAM_STRING_SESSION="your_string_session" \
  telegram_scraper:latest scraper --dry-run
```

**Run Scraper in Production Mode (Yesterday Closed Day):**
```bash
docker run --rm \
  -v $(pwd)/services/telegram_scraper/config.yaml:/app/services/telegram_scraper/config.yaml:ro \
  -e NEON_DATABASE_URL="postgresql://..." \
  telegram_scraper:latest scraper --yesterday
```

**Run Cleaner in Production Mode (Yesterday Closed Day):**
```bash
docker run --rm \
  -e NEON_DATABASE_URL="postgresql://..." \
  telegram_scraper:latest cleaner --yesterday
```

---

## ⚙️ Methods for Managing `config.yaml` on Airflow

To make frequent channel and parameter changes easy without rebuilding Docker images:

1. **Volume / Bind Mount (`DockerOperator`)**:
   Keep `config.yaml` on the host/Airflow VM and mount it with `Mount(source="/opt/airflow/configs/config.yaml", target="/app/services/telegram_scraper/config.yaml", type="bind", read_only=True)`.
2. **Kubernetes ConfigMap (`KubernetesPodOperator`)**:
   Create a ConfigMap (`kubectl create configmap telegram-scraper-config --from-file=config.yaml`) and mount it to `/etc/telegram-scraper/config.yaml` with `CONFIG_PATH=/etc/telegram-scraper/config.yaml`.
3. **Airflow Web UI Variable (Dynamic generation)**:
   Store the YAML directly in Airflow Admin -> Variables as `TELEGRAM_SCRAPER_CONFIG_YAML`. A PythonOperator writes it to disk before the container runs.
4. **Environment Variables**:
   Override specific parameters dynamically via DAG environment variables:
   - `TELEGRAM_CHANNELS="channel1,@channel2,1667647977"`
   - `TELEGRAM_LIMIT_PER_CHANNEL=100`
   - `TELEGRAM_STRING_SESSION="1ApW..."`
   - `CONFIG_PATH="/custom/path/config.yaml"`

Full example Airflow DAGs for all 3 patterns are available in [`services/telegram_scraper/airflow_examples/telegram_scraper_dag.py`](file:///Users/thetpaing/Documents/Coding/test_ai_project/services/telegram_scraper/airflow_examples/telegram_scraper_dag.py).

---

## ⏰ Automation

### 1. Cron Job
Add to crontab (`crontab -e`):
```cron
0 */6 * * * /path/to/test_ai_project/services/telegram_scraper/cron_job.sh
```

### 2. GitHub Actions
The workflow in `.github/workflows/scrape_telegram.yml` triggers every 6 hours automatically once GitHub Secrets (`TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `NEON_DATABASE_URL`) are configured.

