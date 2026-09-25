#!/usr/bin/env python3
"""
services/telegram_scraper/migrate_cleaner_category.py
=====================================================
Database schema migration CLI for Cleaner & Annotation tables:
Safely adds the 'category' VARCHAR(50) column and indexes to existing database tables
(clean_tele_text, cleaning_logs), creates new tables (clean_tele_extra_info, cleaning_error_logs),
and backfills 'category' without dropping tables or losing existing data.

Supports channel normalization (with or without '@').

Usage:
------
    # Run migration on dev environment (public schema)
    python services/telegram_scraper/migrate_cleaner_category.py --env dev

    # Run migration on prod environment (production schema)
    python services/telegram_scraper/migrate_cleaner_category.py --env prod

    # Dry-run inspection (no database changes)
    python services/telegram_scraper/migrate_cleaner_category.py --dry-run
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

from shared.annotation_models import (
    AnnotationBase,
    apply_annotation_migrations,
    _extract_channel_category_mapping,
)
from sqlalchemy import create_engine, inspect, text


def normalize_channel_name(channel: str) -> str:
    """
    Normalize channel name by removing '@', whitespace, and URL prefixes,
    returning the canonical lowercase channel handle.
    """
    ch = str(channel).strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if ch.startswith(prefix):
            ch = ch[len(prefix):]
    return ch.lstrip("@").lower()


def extract_category_mappings(config: dict) -> dict[str, str]:
    """
    Extract canonical mapping from normalized channel name to category name.
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
    """Query distinct channel_name values currently stored in clean_tele_text."""
    is_postgres = engine.dialect.name == "postgresql"
    tbl = f'"{schema}"."clean_tele_text"' if (is_postgres and schema and schema != "public") else '"clean_tele_text"'
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT DISTINCT channel_name FROM {tbl} WHERE channel_name IS NOT NULL")).fetchall()
            return sorted([r[0] for r in rows if r[0]])
    except Exception:
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Safely migrate cleaner database tables to add 'category' column and create extra info / DLQ tables."
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
        print("❌ Error: NEON_DATABASE_URL or postgresql.url not configured.", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print(" 🛠️  Cleaner Database Schema Migration: Add 'category' Column & Tables")
    print("=" * 60)
    print(f" Environment : {env}")
    print(f" Target Schema: {schema}")
    print(f" Mode        : {'DRY-RUN (No changes applied)' if args.dry_run else 'LIVE MIGRATION'}")
    print("=" * 60)

    category_map = extract_category_mappings(config)
    print(f"\n[*] Loaded {len(category_map)} channel-to-category mapping(s) from config:")
    for ch, cat in sorted(category_map.items()):
        print(f"    - {ch} -> {cat}")

    engine = create_engine(pg_url, pool_pre_ping=True)
    if engine.dialect.name == "postgresql" and schema and schema != "public":
        schema_engine = engine.execution_options(schema_translate_map={None: schema})
    else:
        schema_engine = engine

    # Ensure tables exist first
    if not args.dry_run:
        AnnotationBase.metadata.create_all(schema_engine)

    inspector = inspect(schema_engine)
    is_postgres = engine.dialect.name == "postgresql"
    target_tables = ["clean_tele_text", "cleaning_logs", "clean_tele_extra_info", "cleaning_error_logs"]

    print("\n[*] Table & Column Status:")
    for tbl in target_tables:
        t_names = inspector.get_table_names(schema=schema if is_postgres else None)
        if tbl not in t_names:
            print(f"    - {tbl:25s}: NOT FOUND")
            continue
        cols = [c["name"] for c in inspector.get_columns(tbl, schema=schema if is_postgres else None)]
        has_cat = "category" in cols
        status = "OK (has category)" if has_cat else "NEEDS MIGRATION (missing category)"
        print(f"    - {tbl:25s}: {status}")

    # Inspect channels currently in DB
    db_channels = get_distinct_channels_from_db(schema_engine, schema=schema)
    if db_channels:
        print(f"\n[*] Channel Mapping Coverage for existing DB channels ({len(db_channels)} channels):")
        for ch in db_channels:
            cat = match_category(ch, category_map)
            match_status = f"✅ -> {cat}" if cat else "⚠️  -> NULL/Unmapped"
            print(f"    - '{ch}': {match_status}")
    else:
        print("\n[*] No existing rows in clean_tele_text.")

    if args.dry_run:
        print("\n[DRY-RUN] Inspection completed. Exiting without modifying database.")
        sys.exit(0)

    print("\n[*] Applying migrations...")
    results = apply_annotation_migrations(schema_engine, schema=schema, config=config)

    print("\n✅ Migration Finished Successfully:")
    print(f"    - Columns Added            : {results['columns_added'] or 'None (already present)'}")
    print(f"    - Backfilled clean_tele_text : {results['backfilled_clean_text']} rows")
    print(f"    - Backfilled cleaning_logs   : {results['backfilled_cleaning_logs']} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
