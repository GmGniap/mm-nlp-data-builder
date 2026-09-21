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

import logging
import os
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint,
    create_engine, inspect, text,
)
from sqlalchemy.orm import declarative_base

logger = logging.getLogger(__name__)

ScraperBase = declarative_base()


class TelegramMessage(ScraperBase):
    """Raw Telegram channel post as scraped by scraper.py."""
    __tablename__ = "telegram_messages"
    __table_args__ = (
        UniqueConstraint("channel_name", "message_id", name="uq_channel_message"),
    )

    id           = Column(Integer, primary_key=True)
    channel_name = Column(String(100), nullable=False, index=True)
    # Category separated in config
    category     = Column(String(50), nullable=True, index=True)
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
    # Category separated in config
    category         = Column(String(50), nullable=True, index=True)
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


class ScrapingErrorLog(ScraperBase):
    """DLQ table recording channel scraping failures, error details, and retry status."""
    __tablename__ = "scraping_error_logs"

    id             = Column(Integer, primary_key=True)
    channel_name   = Column(String(100), nullable=False, index=True)
    # Category separated in config
    category       = Column(String(50), nullable=True, index=True)
    run_date       = Column(String(10), nullable=False, index=True)
    error_type     = Column(String(100), nullable=False)
    error_message  = Column(Text, nullable=False)
    stack_trace    = Column(Text, nullable=True)
    retry_count    = Column(Integer, default=0)
    resolved       = Column(Boolean, default=False)
    created_at     = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<ScrapingErrorLog {self.channel_name}:{self.run_date} error={self.error_type}>"


class ArchivalLog(ScraperBase):
    """Audit log tracking cold storage Parquet archival jobs and DB purge status."""
    __tablename__ = "archival_logs"

    id              = Column(Integer, primary_key=True)
    table_name      = Column(String(50), nullable=False, index=True)
    channel_name    = Column(String(100), nullable=True)
    # Category separated in config
    category        = Column(String(50), nullable=True, index=True)
    cutoff_date     = Column(DateTime, nullable=False, index=True)
    s3_uri          = Column(String(500), nullable=False)
    rows_archived   = Column(Integer, default=0)
    file_size_bytes = Column(BigInteger, default=0)
    status          = Column(String(20), nullable=False, default="in_progress")
    error_message   = Column(Text, nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)
    finished_at     = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<ArchivalLog {self.table_name} s3={self.s3_uri} status={self.status}>"


def _extract_channel_category_mapping(config: dict | None = None) -> dict[str, str]:
    """Extract mapping of channel variations -> category name from config dict or file."""
    if config is None:
        try:
            import yaml
            base_dir = os.path.dirname(__file__)
            cfg_candidates = [
                os.path.join(base_dir, "../services/telegram_scraper/config.yaml"),
                "/app/services/telegram_scraper/config.yaml",
                "services/telegram_scraper/config.yaml",
                os.path.join(base_dir, "../services/telegram_scraper/config.example.yaml"),
            ]
            for cp in cfg_candidates:
                if os.path.exists(cp):
                    with open(cp, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)
                    break
        except Exception as e:
            logger.debug(f"Could not load config for category backfill: {e}")
            config = None

    channel_to_category: dict[str, str] = {}
    if not config or not isinstance(config, dict):
        return channel_to_category

    scraping_cfg = config.get("scraping", {})
    categories_cfg = scraping_cfg.get("categories", {})
    if isinstance(categories_cfg, dict):
        for cat_name, cat_val in categories_cfg.items():
            ch_list = cat_val.get("channels", []) if isinstance(cat_val, dict) else (
                cat_val if isinstance(cat_val, list) else []
            )
            for ch in ch_list:
                ch_str = str(ch).strip()
                bare = ch_str.lstrip("@")
                channel_to_category[ch_str] = str(cat_name)
                channel_to_category[bare] = str(cat_name)
                channel_to_category[f"@{bare}"] = str(cat_name)

    channels_cfg = scraping_cfg.get("channels", {})
    if isinstance(channels_cfg, dict):
        for cat_name, ch_list in channels_cfg.items():
            if isinstance(ch_list, list):
                for ch in ch_list:
                    ch_str = str(ch).strip()
                    bare = ch_str.lstrip("@")
                    channel_to_category[ch_str] = str(cat_name)
                    channel_to_category[bare] = str(cat_name)
                    channel_to_category[f"@{bare}"] = str(cat_name)

    return channel_to_category


def apply_scraper_migrations(engine, schema: str = "public", config: dict | None = None) -> dict:
    """
    Safely apply schema migrations to add 'category' column to scraper tables if missing.
    Zero-downtime, no table drops, and no data loss.
    Backfills existing rows where category is NULL based on channels defined in config.yaml.
    """
    results = {"columns_added": [], "backfilled_messages": 0, "backfilled_logs": 0}
    is_postgres = engine.dialect.name == "postgresql"
    inspector = inspect(engine)

    target_tables = ["telegram_messages", "scraping_logs", "scraping_error_logs", "archival_logs"]

    with engine.connect() as conn:
        for tbl in target_tables:
            # Check if table exists
            table_names = inspector.get_table_names(schema=schema if is_postgres else None)
            if tbl not in table_names:
                continue

            columns = [col["name"] for col in inspector.get_columns(tbl, schema=schema if is_postgres else None)]
            if "category" not in columns:
                if is_postgres:
                    qual_tbl = f'"{schema}"."{tbl}"' if (schema and schema != "public") else f'"{tbl}"'
                    conn.execute(text(f'ALTER TABLE {qual_tbl} ADD COLUMN IF NOT EXISTS category VARCHAR(50)'))
                    conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{tbl}_category" ON {qual_tbl} (category)'))
                else:
                    # SQLite dialect
                    conn.execute(text(f'ALTER TABLE "{tbl}" ADD COLUMN category VARCHAR(50)'))
                    conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{tbl}_category" ON "{tbl}" (category)'))
                results["columns_added"].append(tbl)
        conn.commit()

    # Backfill historical records where category is NULL or empty
    channel_map = _extract_channel_category_mapping(config)
    with engine.connect() as conn:
        for ch, cat in channel_map.items():
            bare = ch.lstrip("@").lower()
            at_bare = f"@{bare}"
            if is_postgres:
                msg_tbl = f'"{schema}"."telegram_messages"' if (schema and schema != "public") else '"telegram_messages"'
                log_tbl = f'"{schema}"."scraping_logs"' if (schema and schema != "public") else '"scraping_logs"'
            else:
                msg_tbl = '"telegram_messages"'
                log_tbl = '"scraping_logs"'

            res_m = conn.execute(
                text(f"""UPDATE {msg_tbl} SET category = :cat
                         WHERE (lower(ltrim(channel_name, '@')) = :bare OR channel_name = :ch OR channel_name = :at_bare)
                         AND (category IS NULL OR category = '')"""),
                {"cat": cat, "bare": bare, "ch": ch, "at_bare": at_bare}
            )
            res_l = conn.execute(
                text(f"""UPDATE {log_tbl} SET category = :cat
                         WHERE (lower(ltrim(channel_name, '@')) = :bare OR channel_name = :ch OR channel_name = :at_bare)
                         AND (category IS NULL OR category = '')"""),
                {"cat": cat, "bare": bare, "ch": ch, "at_bare": at_bare}
            )
            results["backfilled_messages"] += res_m.rowcount or 0
            results["backfilled_logs"] += res_l.rowcount or 0

        # Fill any remaining unmapped NULL category rows with empty blank value ''
        if is_postgres:
            msg_tbl = f'"{schema}"."telegram_messages"' if (schema and schema != "public") else '"telegram_messages"'
            log_tbl = f'"{schema}"."scraping_logs"' if (schema and schema != "public") else '"scraping_logs"'
        else:
            msg_tbl = '"telegram_messages"'
            log_tbl = '"scraping_logs"'

        conn.execute(text(f"UPDATE {msg_tbl} SET category = '' WHERE category IS NULL"))
        conn.execute(text(f"UPDATE {log_tbl} SET category = '' WHERE category IS NULL"))
        conn.commit()

    return results


def init_scraper_db(pg_url: str, schema: str = "public", config: dict | None = None):
    """Connect to Neon PostgreSQL and create scraper-owned tables only in the target schema."""
    engine = create_engine(pg_url, pool_pre_ping=True)
    if engine.dialect.name == "postgresql":
        if schema and schema != "public":
            with engine.connect() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                conn.commit()
        schema_engine = engine.execution_options(schema_translate_map={None: schema})
    else:
        schema_engine = engine
    ScraperBase.metadata.create_all(schema_engine)
    apply_scraper_migrations(schema_engine, schema=schema, config=config)
    return schema_engine
