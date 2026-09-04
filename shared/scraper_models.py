"""
shared/scraper_models.py
========================
SQLAlchemy models for the Telegram Scraper service.

Tables owned by this module (all in Neon PostgreSQL):
  - telegram_messages   raw scraped Telegram posts
  - scraping_logs       per-run audit trail / watermark log

Usage
-----
    from shared.scraper_models import TelegramMessage, ScrapingLog, init_scraper_db

    engine = init_scraper_db("postgresql://user:pw@host/db?sslmode=require")

Only import this module in the scraper service (scraper.py, storage.py, cleaner.py).
The Flask annotation app must NOT import these models.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, DateTime, Integer, String, Text, UniqueConstraint,
    create_engine, text,
)
from sqlalchemy.orm import declarative_base

ScraperBase = declarative_base()


class TelegramMessage(ScraperBase):
    """Raw Telegram channel post as scraped by scraper.py."""
    __tablename__ = "telegram_messages"
    __table_args__ = (
        UniqueConstraint("channel_name", "message_id", name="uq_channel_message"),
    )

    id           = Column(Integer, primary_key=True)
    channel_name = Column(String(100), nullable=False, index=True)
    message_id   = Column(BigInteger, nullable=False, index=True)
    message_text = Column(Text, nullable=False)
    date         = Column(DateTime, nullable=True)
    media_url    = Column(String(500), nullable=True)
    status       = Column(String(20), default="pending", index=True)
    created_at   = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<TelegramMessage {self.channel_name}:{self.message_id}>"


class ScrapingLog(ScraperBase):
    """One row per scraping run (day-watermark boundary) for a specific channel."""
    __tablename__ = "scraping_logs"
    __table_args__ = (
        UniqueConstraint("channel_name", "run_date", name="uq_channel_rundate"),
    )

    id               = Column(Integer, primary_key=True)
    channel_name     = Column(String(100), nullable=False, index=True)
    run_date         = Column(String(10), nullable=False, index=True)
    messages_scraped = Column(Integer, default=0)
    messages_saved   = Column(Integer, default=0)
    messages_skipped = Column(Integer, default=0)
    scrape_start_ts  = Column(DateTime, nullable=False)
    scrape_end_ts    = Column(DateTime, nullable=False)
    run_started_at   = Column(DateTime, default=datetime.utcnow)
    run_finished_at  = Column(DateTime, nullable=True)
    status           = Column(String(20), default="running")

    def __repr__(self) -> str:
        return f"<ScrapingLog {self.run_date} status={self.status}>"


def init_scraper_db(pg_url: str, schema: str = "public"):
    """Connect to Neon PostgreSQL and create scraper-owned tables only in the target schema."""
    engine = create_engine(pg_url, pool_pre_ping=True)
    if schema and schema != "public":
        with engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()
    schema_engine = engine.execution_options(schema_translate_map={None: schema})
    ScraperBase.metadata.create_all(schema_engine)
    return schema_engine
