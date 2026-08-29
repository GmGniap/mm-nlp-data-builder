#!/usr/bin/env bash
# Script for local or server cron job automation of Telegram Scraping (managed by uv)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../" &> /dev/null && pwd )"

echo "=== Telegram Scraper Cron Job Started: $(date) ==="

cd "$PROJECT_ROOT"

if command -v uv &> /dev/null; then
    uv run python3 "$SCRIPT_DIR/scraper.py" >> "$SCRIPT_DIR/scraper_cron.log" 2>&1
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
    python3 "$SCRIPT_DIR/scraper.py" >> "$SCRIPT_DIR/scraper_cron.log" 2>&1
else
    python3 "$SCRIPT_DIR/scraper.py" >> "$SCRIPT_DIR/scraper_cron.log" 2>&1
fi

echo "=== Telegram Scraper Cron Job Finished: $(date) ==="
