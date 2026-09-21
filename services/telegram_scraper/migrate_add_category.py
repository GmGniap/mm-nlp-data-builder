#!/usr/bin/env python3
"""
services/telegram_scraper/migrate_add_category.py
=================================================
Database schema migration CLI:
Safely adds the 'category' VARCHAR(50) column and index to existing database tables
(telegram_messages, scraping_logs, scraping_error_logs, archival_logs) without dropping tables
or losing existing data.

Correctly maps between channel_name and category regardless of whether the channel name
contains the '@' symbol in config.yaml or in the database rows.

Usage:
------
    # Run migration on dev environment (public schema)
    python services/telegram_scraper/migrate_add_category.py --env dev

    # Run migration on prod environment (production schema)
    python services/telegram_scraper/migrate_add_category.py --env prod

    # Dry-run inspection (no database changes)
    python services/telegram_scraper/migrate_add_category.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.scraper_models import (
    ScraperBase,
    apply_scraper_migrations,
)
from sqlalchemy import create_engine, inspect, text


def normalize_channel_name(channel: str) -> str:
    """
    Normalize channel name by removing '@', whitespace, and URL prefixes,
    returning the canonical lowercase channel handle.

    Examples:
        '@khitthitnews'           -> 'khitthitnews'
        'khitthitnews'            -> 'khitthitnews'
        '@SittKhwayDead'          -> 'sittkhwaydead'
        'https://t.me/shweba000'  -> 'shweba000'
    """
    ch = str(channel).strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if ch.startswith(prefix):
            ch = ch[len(prefix):]
    return ch.lstrip("@").lower()


def extract_category_mappings(config: dict) -> dict[str, str]:
    """
    Extract canonical mapping from normalized channel name (without '@') to category name.
    Supports both nested scraping.categories.<cat>.channels and legacy scraping.channels.<cat>.
    """
    scraping_cfg = config.get("scraping", {})
    categories_cfg = scraping_cfg.get("categories", {})
    channels_cfg = scraping_cfg.get("channels", {})
    mapping: dict[str, str] = {}

    if isinstance(categories_cfg, dict):
        for cat_name, cat_val in categories_cfg.items():
            ch_list = cat_val.get("channels", []) if isinstance(cat_val, dict) else (
                cat_val if isinstance(cat_val, list) else []
            )
            for ch in ch_list:
                norm = normalize_channel_name(ch)
                if norm:
                    mapping[norm] = str(cat_name)

    if isinstance(channels_cfg, dict):
        for cat_name, ch_list in channels_cfg.items():
            if isinstance(ch_list, list):
                for ch in ch_list:
                    norm = normalize_channel_name(ch)
                    if norm:
                        mapping[norm] = str(cat_name)

    return mapping


def match_category(channel_name: str, category_map: dict[str, str]) -> str | None:
    """
    Resolve category for a channel name with or without '@' symbol.
    """
    norm = normalize_channel_name(channel_name)
    return category_map.get(norm)


def load_config(config_path: str | Path | None = None) -> dict:
    candidates = [
        Path(config_path) if config_path else None,
        BASE_DIR / "config.yaml",
        BASE_DIR / "config.example.yaml",
    ]
    for p in candidates:
        if p and p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    return {}


def resolve_env_and_schema(config: dict, env_override: str | None = None) -> tuple[str, str]:
    env = (env_override or os.getenv("ENVIRONMENT") or config.get("environment", "dev")).lower()
    schema = "production" if env == "prod" else "public"
    return env, schema


def get_distinct_channels_from_db(engine, schema: str = "public") -> list[str]:
    """Query distinct channel_name values currently stored in the database."""
    is_postgres = engine.dialect.name == "postgresql"
    msg_tbl = f'"{schema}"."telegram_messages"' if (is_postgres and schema and schema != "public") else '"telegram_messages"'
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT DISTINCT channel_name FROM {msg_tbl} WHERE channel_name IS NOT NULL")).fetchall()
            return sorted([r[0] for r in rows if r[0]])
    except Exception:
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Safely migrate database to add 'category' column and map channel_name with or without '@'."
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml (default: services/telegram_scraper/config.yaml)")
    parser.add_argument("--env", choices=["dev", "prod"], default=None, help="Target environment ('dev' -> public, 'prod' -> production)")
    parser.add_argument("--dry-run", action="store_true", help="Inspect and report without modifying the database")
    args = parser.parse_args()

    load_dotenv()

    config = load_config(args.config)
    env, schema = resolve_env_and_schema(config, args.env)
    pg_url = os.getenv("NEON_DATABASE_URL") or config.get("postgresql", {}).get("url")

    if not pg_url:
        print("❌ Error: PostgreSQL URL not found in NEON_DATABASE_URL or config.yaml")
        sys.exit(1)

    print("=" * 70)
    print("  🚀 Telegram Scraper Database Migration: Channel & Category Mapping")
    print("=" * 70)
    print(f"  Target Environment : {env}")
    print(f"  PostgreSQL Schema  : {schema}")
    print(f"  Dry-run Mode       : {args.dry_run}")
    print("=" * 70)

    engine = create_engine(pg_url, pool_pre_ping=True)
    if engine.dialect.name == "postgresql" and schema and schema != "public":
        schema_engine = engine.execution_options(schema_translate_map={None: schema})
    else:
        schema_engine = engine

    inspector = inspect(schema_engine)
    target_tables = ["telegram_messages", "scraping_logs", "scraping_error_logs", "archival_logs"]
    category_map = extract_category_mappings(config)

    print("\n🔍 Checking existing tables:")
    existing_tables = inspector.get_table_names(schema=schema if engine.dialect.name == "postgresql" else None)
    for tbl in target_tables:
        if tbl in existing_tables:
            cols = [c["name"] for c in inspector.get_columns(tbl, schema=schema if engine.dialect.name == "postgresql" else None)]
            has_cat = "category" in cols
            status = "✅ 'category' present" if has_cat else "⚠️  'category' missing (needs migration)"
            print(f"  - {tbl:22}: {status}")
        else:
            print(f"  - {tbl:22}: ℹ️  Table does not exist yet (will be created on first scraper run)")

    print(f"\n📋 Configured Channels ({len(category_map)} canonical channels):")
    print(f"  {'Canonical Channel':<25} {'Variants (w/ & w/o @)':<30} {'Category'}")
    print("  " + "-" * 68)
    for bare_name, cat in sorted(category_map.items()):
        variants = f"{bare_name}, @{bare_name}"
        print(f"  {bare_name:<25} {variants:<30} {cat}")

    db_channels = get_distinct_channels_from_db(schema_engine, schema=schema)
    if db_channels:
        print(f"\n🔎 Reconciling Existing Database Channels ({len(db_channels)} distinct found):")
        print(f"  {'DB Channel Name':<25} {'Has @':<8} {'Canonical Match':<20} {'Assigned Category'}")
        print("  " + "-" * 68)
        for db_ch in db_channels:
            has_at = "Yes" if str(db_ch).startswith("@") else "No"
            norm = normalize_channel_name(db_ch)
            cat = match_category(db_ch, category_map)
            status_cat = cat if cat else "(blank / unmapped)"
            print(f"  {str(db_ch):<25} {has_at:<8} {norm:<20} {status_cat}")
    else:
        print("\nℹ️  No historical channel messages found in database.")

    if args.dry_run:
        print("\n[DRY-RUN] No changes were written to the database.")
        sys.exit(0)

    print("\n⚡ Executing safe DDL migration and backfill...")
    results = apply_scraper_migrations(schema_engine, schema=schema, config=config)

    print("\n✅ Migration Completed Successfully:")
    if results["columns_added"]:
        print(f"  - Added 'category' column to: {', '.join(results['columns_added'])}")
    else:
        print("  - All existing tables already have 'category' column.")
    print(f"  - Historical telegram_messages backfilled: {results['backfilled_messages']}")
    print(f"  - Historical scraping_logs backfilled   : {results['backfilled_logs']}")

    # Display post-migration distribution in database
    try:
        is_postgres = schema_engine.dialect.name == "postgresql"
        msg_tbl = f'"{schema}"."telegram_messages"' if (is_postgres and schema and schema != "public") else '"telegram_messages"'
        with schema_engine.connect() as conn:
            counts = conn.execute(text(f"""
                SELECT COALESCE(NULLIF(category, ''), '(blank)') as cat, count(*) 
                FROM {msg_tbl} 
                GROUP BY cat 
                ORDER BY count(*) DESC
            """)).fetchall()
            print(f"\n📊 Current Category Breakdown in {msg_tbl}:")
            for c_name, count in counts:
                print(f"  - {c_name:<16}: {count} messages")
    except Exception as e:
        pass

    print("=" * 70)


if __name__ == "__main__":
    main()
