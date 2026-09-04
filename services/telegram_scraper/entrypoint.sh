#!/usr/bin/env bash
set -e

# Support flexible commands
case "$1" in
    scraper)
        shift
        exec python /app/services/telegram_scraper/scraper.py "$@"
        ;;
    cleaner)
        shift
        exec python /app/services/telegram_scraper/cleaner.py "$@"
        ;;
    login)
        shift
        exec python /app/services/telegram_scraper/login_telegram.py "$@"
        ;;
    *)
        # If user passes custom args like "--lookback 7" directly or a shell command
        if [[ "$1" == --* ]] || [ -z "$1" ]; then
            exec python /app/services/telegram_scraper/scraper.py "$@"
        else
            exec "$@"
        fi
        ;;
esac
