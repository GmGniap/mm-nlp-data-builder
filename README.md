# MM NLP Data Builder

This repository is aimed to created required tools for Myanmar NLP tasks such as data annotation or data collection.

---

## 🏛️ Workspace Structure

```
.
├── pyproject.toml                 # uv Project configuration (Python 3.13 dependencies)
├── README.md                      # Overall project documentation and subagent roadmap
├── .gitignore                     # Repository gitignore policies
│
├── architect/                     # Main Architect Docs
│   ├── PROJECT_OVERVIEW.md        # System architecture, infra, data flow, DB choices
│   ├── PROJECT_COMPONENTS.md      # Detailed component breakdown & API contracts
│   ├── SUBAGENT_INSTRUCTIONS.md   # SOP and prompt guidelines for Agent 2 & Agent 3
│   └── SKILLS_AND_RESOURCES.md    # Tech stack references, uv guidelines & NLP tips
│
├── shared/                        # 🔄 Shared Data Schemas & Models
│   └── models.py                  # Shared SQLAlchemy database models for messages & annotations
│
├── services/
│   ├── telegram_scraper/          # Python Developer Agent (Telegram Scraping)
│   │   ├── config.example.yaml    # Config-driven target channels & fetch parameters
│   │   ├── scraper.py             # Telethon/Pyrogram scraper implementation
│   │   ├── storage.py             # DB persistence adapter
│   │   ├── cron_job.sh            # Cron job automation runner (uv enabled)
│   │   ├── README.md              # Agent 2 developer guide
│   │   └── requirements.txt       # Telegram scraping dependencies
│   │
│   └── nlp_annotation_app/        # Full-Stack Developer Agent (NLP Annotation UI)
│       ├── app.py                 # Flask web application entrypoint (Arloo integrated)
│       ├── models.py              # Application models (Users, Annotations, Scraped Data)
│       ├── templates/             # UI Templates (login, register, dashboard, annotate)
│       ├── README.md              # Agent 3 developer guide
│       └── requirements.txt       # Web UI & NLP dependencies
│
└── .github/
    └── workflows/
        └── scrape_telegram.yml    # GitHub Actions workflow for scheduled automated scraping
```

---

## ⚡ Quick Start with `uv` & Python 3.13

### 1. Create Python 3.13 Environment
```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
```

### 2. Install Project Dependencies
```bash
uv pip install -r services/telegram_scraper/requirements.txt -r services/nlp_annotation_app/requirements.txt
```

### 3. Run Telegram Scraper
```bash
uv run python services/telegram_scraper/scraper.py --dry-run
```

### 4. Run Flask Annotation Platform
```bash
uv run python services/nlp_annotation_app/app.py
```
App will start on `http://127.0.0.1:5000`.
