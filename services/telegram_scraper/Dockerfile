# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    CONFIG_PATH=/app/services/telegram_scraper/config.yaml

# Install system dependencies (libpq for psycopg2, curl/ca-certificates)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency definition and install
COPY services/telegram_scraper/requirements.txt /app/services/telegram_scraper/requirements.txt
RUN pip install --no-cache-dir -r /app/services/telegram_scraper/requirements.txt

# Copy shared models and scraper codebase
COPY shared /app/shared
COPY services/telegram_scraper /app/services/telegram_scraper

# Create directory for session and runtime data
RUN mkdir -p /app/services/telegram_scraper/data && \
    chmod -R 777 /app/services/telegram_scraper/data

# Entrypoint script
COPY services/telegram_scraper/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["scraper"]
