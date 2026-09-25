"""
cleaner.py — Myanmar Sentence Cleaner & Annotation DB Uploader
===============================================================
Pipeline:
  1. Read TelegramMessage rows from Neon PostgreSQL (scraper DB) for a given
     channel and day window (filtered by TelegramMessage.channel_name and date).
  2. Apply category-specific data transformations:
     - News: Extract headline, dateline location/date short_note, external URLs,
       and split body into clean Myanmar sentences. Headline is stored in both
       clean_tele_extra_info and clean_tele_text (line_index=0).
     - Polarization: In-memory same-channel same-day deduplication, channel
       discovery logging ([DISCOVERY]), emoji/noise stripping, English-only
       sentence drop, and < 8 syllable/word filter.
  3. Write CleanTeleText, CleanTeleExtraInfo, and CleaningLog rows to Neon PostgreSQL.
  4. Provide file-based staging (--stage-dir) and category bulk upload (--upload-staged)
     to avoid Neon DB connection and compute saturation under Airflow dynamic task mapping.
  5. Isolate message-level errors into Dead Letter Queue (CleaningErrorLog) and provide
     manual replay (--retry-dlq).

Usage:
-----
    # Default incremental: cleans all channels after their latest watermark date
    uv run python services/telegram_scraper/cleaner.py

    # Clean a single channel for yesterday (Airflow daily batch)
    uv run python services/telegram_scraper/cleaner.py --channel shweba000 --category polarization --yesterday

    # Stage to local files (avoids direct DB insert spikes in Airflow)
    uv run python services/telegram_scraper/cleaner.py --category news --channel khitthitnews --yesterday --stage-dir data/clean_staging

    # Category barrier bulk upload from staging files
    uv run python services/telegram_scraper/cleaner.py --category news --upload-staged --stage-dir data/clean_staging

    # Replay failed messages from DLQ table
    uv run python services/telegram_scraper/cleaner.py --retry-dlq --category polarization
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import traceback
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "../../"))
sys.path.insert(0, PROJECT_ROOT)

# Scraper models — read TelegramMessage rows
from shared.scraper_models import TelegramMessage

# Annotation models — write CleanTeleText, CleanTeleExtraInfo, CleaningLog, CleaningErrorLog rows
from shared.annotation_models import (
    AnnotationBase,
    CleanTeleText,
    CleanTeleExtraInfo,
    CleaningLog,
    CleaningErrorLog,
    AnnotationResult,
    init_annotation_db,
)

load_dotenv()

logging.basicConfig(
    filename="output.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ===========================================================================
# Dictionaries & Mappings for Myanmar Normalization
# ===========================================================================

MYANMAR_DIGITS_MAP: dict[str, str] = {
    "၀": "0", "၁": "1", "၂": "2", "၃": "3", "၄": "4",
    "၅": "5", "၆": "6", "၇": "7", "၈": "8", "၉": "9",
}

MYANMAR_TO_ENG_MONTHS: dict[str, str] = {
    "ဇန်နဝါရီ": "January",
    "ဖေဖော်ဝါရီ": "February",
    "မတ်": "March",
    "ဧပြီ": "April",
    "မေ": "May",
    "ဇွန်": "June",
    "ဇူလိုင်": "July",
    "သြဂုတ်": "August",
    "ဩဂုတ်": "August",
    "စက်တင်ဘာ": "September",
    "အောက်တိုဘာ": "October",
    "နိုဝင်ဘာ": "November",
    "ဒီဇင်ဘာ": "December",
}

ENG_MONTH_TO_NUM: dict[str, int] = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def normalize_myanmar_digits(text_str: str) -> str:
    """Convert Myanmar digits (၀-၉) to Western digits (0-9)."""
    return "".join(MYANMAR_DIGITS_MAP.get(ch, ch) for ch in text_str)


def standardize_news_date(date_raw: str, run_date: str = "") -> str | None:
    """
    Standardize a date string (Myanmar or English, e.g. 'စက်တင်ဘာ ၂၀ ရက်' or '24 Sep 2026 By Khaosod English')
    into a standardized 'YYYY-MM-DD' date string.
    Year is resolved from date_raw, run_date, or scraping time (current year).
    """
    if not date_raw or not date_raw.strip():
        return None

    norm = normalize_myanmar_digits(date_raw.strip())

    # Check if already in standard ISO format YYYY-MM-DD
    iso_match = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", norm)
    if iso_match:
        try:
            y, m, d = int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))
            return datetime(y, m, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Check DD-MM-YYYY or DD/MM/YYYY
    dmy_match = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b", norm)
    if dmy_match:
        try:
            d, m, y = int(dmy_match.group(1)), int(dmy_match.group(2)), int(dmy_match.group(3))
            return datetime(y, m, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Check English formatted date: DD Month YYYY (e.g. '23 Sep 2026' or '24 September 2026')
    eng_dmy_match = re.search(r"\b(\d{1,2})[-/\s]+([A-Za-z]+)[-/\s]+(\d{4})\b", norm)
    if eng_dmy_match:
        d = int(eng_dmy_match.group(1))
        m_str = eng_dmy_match.group(2).capitalize()
        y = int(eng_dmy_match.group(3))
        m = ENG_MONTH_TO_NUM.get(m_str) or ENG_MONTH_TO_NUM.get(m_str[:3])
        if m and 1 <= d <= 31:
            try:
                return datetime(y, m, d).strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Check English formatted date: Month DD, YYYY (e.g. 'Sep 23, 2026' or 'September 24, 2026')
    eng_mdy_match = re.search(r"\b([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\b", norm)
    if eng_mdy_match:
        m_str = eng_mdy_match.group(1).capitalize()
        d = int(eng_mdy_match.group(2))
        y = int(eng_mdy_match.group(3))
        m = ENG_MONTH_TO_NUM.get(m_str) or ENG_MONTH_TO_NUM.get(m_str[:3])
        if m and 1 <= d <= 31:
            try:
                return datetime(y, m, d).strftime("%Y-%m-%d")
            except ValueError:
                pass

    matched_month: str | None = None
    month_num: int | None = None
    for mm_month, eng_month in MYANMAR_TO_ENG_MONTHS.items():
        if mm_month in date_raw:
            matched_month = eng_month
            month_num = ENG_MONTH_TO_NUM[eng_month]
            norm = norm.replace(mm_month, eng_month)
            break

    # Strip Myanmar date suffixes/particles
    norm = re.sub(r"\s*ရက်\b", "", norm)
    norm = re.sub(r"\s*ခုနှစ်\b", "", norm)

    # 1. Resolve 4-digit year: from text, run_date, or current scraping year
    year: int | None = None
    year_match = re.search(r"\b(19\d\d|20\d\d)\b", norm)
    if year_match:
        year = int(year_match.group(1))
    elif run_date:
        run_year_match = re.match(r"^(\d{4})", run_date.strip())
        if run_year_match:
            year = int(run_year_match.group(1))

    if year is None:
        # Fall back to current year (scraping time assumption)
        year = datetime.now(timezone.utc).year

    # 2. Resolve day (1-31)
    day: int | None = None
    temp_norm = re.sub(r"\b(19\d\d|20\d\d)\b", "", norm) if year_match else norm
    day_match = re.search(r"\b([0-3]?[0-9])\b", temp_norm)
    if day_match:
        d = int(day_match.group(1))
        if 1 <= d <= 31:
            day = d

    # 3. Format strictly to YYYY-MM-DD
    if year and month_num and day:
        try:
            dt = datetime(year, month_num, day)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    return None


# ===========================================================================
# Config loader & Environment Helper
# ===========================================================================

def load_config(config_file: str | None = None) -> dict:
    if not config_file:
        config_file = os.getenv("CONFIG_PATH", "config.yaml")
    if os.path.isabs(config_file):
        target = config_file
    else:
        target = os.path.join(BASE_DIR, config_file)
    if not os.path.exists(target):
        target = os.path.join(BASE_DIR, "config.example.yaml")
    with open(target, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_environment_and_schema(config: dict, env_override: str | None = None) -> tuple[str, str]:
    """
    Resolve environment ('dev' or 'prod') and corresponding PostgreSQL schema
    ('public' or 'production').
    """
    env = env_override or os.getenv("ENVIRONMENT") or config.get("environment", "dev")
    env = str(env).strip().lower()
    if env in ("prod", "production"):
        return "prod", "production"
    return "dev", "public"


def get_known_channels(config: dict) -> set[str]:
    """Collect all known channel handles from config.yaml as a normalized set."""
    scraping_cfg = config.get("scraping", {})
    categories = scraping_cfg.get("categories", {})
    channels_cfg = scraping_cfg.get("channels", {})
    known: set[str] = set()

    if isinstance(categories, dict):
        for _, cat_val in categories.items():
            ch_list = cat_val.get("channels", []) if isinstance(cat_val, dict) else (
                cat_val if isinstance(cat_val, list) else []
            )
            for ch in ch_list:
                known.add(str(ch).lstrip("@").strip().lower())

    if isinstance(channels_cfg, dict):
        for _, ch_list in channels_cfg.items():
            if isinstance(ch_list, list):
                for ch in ch_list:
                    known.add(str(ch).lstrip("@").strip().lower())

    return known


# ===========================================================================
# Myanmar sentence splitter  (port of my-linebreak.pl by Dr.Ye Kyaw Thu)
# ===========================================================================

_DEFAULT_BIGRAMS: list[str] = [
    # Bare potema (sentence-ending punctuation)
    "။", "၊",
    # Common sentence-final clitics + full stop ။
    "ပြီ ။", "လို့ ။", "တယ် ။", "ဘူး ။", "မယ် ။",
    "ကြ ။",  "ဆို ။", "ပဲ ။",  "နဲ့ ။", "ဖြစ် ။",
    "ရ ။",   "ခဲ့ ။", "ကို ။", "တဲ့ ။", "ဒါ ။",
    "လဲ ။",  "ပါ ။",  "နော် ။","မဟုတ် ။",
    "သည် ။", "၏ ။",  "တော် ။","ပေ ။",  "ဟု ။",
    "ဟဲ့ ။", "ချည် ။","ဖြင့် ။","ထဲ ။",
    # Clause-level breaks on ၊
    "ပြီး ၊", "ပြီ ၊",  "တော့ ၊", "ဆို ၊",
    "တဲ့ ၊",  "ကို ၊",  "မှာ ၊",  "နဲ့ ၊",
]


def _build_split_pattern(bigrams: list[str]) -> re.Pattern:
    """Compile a regex that matches any bigram and captures it (group 1)."""
    sorted_bigrams = sorted(bigrams, key=len, reverse=True)
    pattern_str = "(" + "|".join(re.escape(b) for b in sorted_bigrams) + ")"
    return re.compile(pattern_str, re.UNICODE)


def _load_dict_file(dict_path: str) -> list[str]:
    with open(dict_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def is_valid_sentence(segment: str) -> bool:
    """Return True if segment contains at least one letter or digit."""
    return any(
        unicodedata.category(ch)[0] in {"L", "N"}
        for ch in segment
    )


def split_myanmar_sentences(
    paragraph: str,
    bigrams: list[str] | None = None,
) -> list[str]:
    """Split a Myanmar paragraph into individual sentences using Dr. Ye Kyaw Thu algorithm."""
    if not paragraph or not paragraph.strip():
        return []

    effective_bigrams = bigrams if bigrams is not None else _DEFAULT_BIGRAMS
    pattern = _build_split_pattern(effective_bigrams)
    sentences: list[str] = []

    for raw_line in paragraph.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r" +", " ", line)
        line = pattern.sub(r"\1\n", line)
        for segment in line.split("\n"):
            segment = segment.strip()
            if segment and is_valid_sentence(segment):
                sentences.append(segment)

    return sentences


# ===========================================================================
# Advanced Category-Specific Transformation Engines
# ===========================================================================

def parse_news_message(raw_text: str, run_date: str = "") -> tuple[str | None, str | None, str | None, list[str], str]:
    """
    Parse a news post into:
      - headline: Line 1 of the article.
      - clean_info_date: Standardized date YYYY-MM-DD format (e.g. '2026-09-20'). Year can be extracted from scraping time while assuming the news is published within this year.
      - original_short_note: Original Line 2 text (e.g. 'မကွေး၊ စက်တင်ဘာ ၂၀ ရက်').
      - url_lists: List of external URLs extracted from message.
      - body_text: Remaining text lines to be split into sentences.
    """
    if not raw_text or not raw_text.strip():
        return None, None, None, [], ""

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return None, None, None, [], ""

    headline = lines[0]
    clean_info_date = None
    original_short_note = None
    body_start_idx = 1

    # Extract all external URLs from raw_text
    urls = re.findall(r"https?://[^\s]+", raw_text)
    url_lists = list(dict.fromkeys(urls))

    if len(lines) > 1:
        line2 = lines[1]
        dateline_match = re.search(r"^([^\n\r၊,]+)[၊,]\s*([^\n\r]+)", line2)
        if dateline_match:
            date_raw = dateline_match.group(2).strip()
            clean_date = standardize_news_date(date_raw, run_date=run_date)
            if clean_date:
                clean_info_date = clean_date
                original_short_note = line2
                body_start_idx = 2
        else:
            # Check for English dateline format: 2 digits + words (month) + 4 digits (e.g. "23 Sep 2026 By MPA", "24 Sep 2026 By Khaosod English")
            eng_date_match = re.search(r"\b([\d၀-၉]{1,2}[-/\s]+[A-Za-z]+[-/\s]+[\d၀-၉]{4})\b", line2)
            if eng_date_match:
                date_raw = eng_date_match.group(1).strip()
                clean_date = standardize_news_date(date_raw, run_date=run_date)
                if clean_date:
                    clean_info_date = clean_date
                    original_short_note = line2
                    body_start_idx = 2
            else:
                # Check if line2 directly matches a date without location/comma
                has_month = any(mm in line2 for mm in MYANMAR_TO_ENG_MONTHS)
                if has_month:
                    clean_date = standardize_news_date(line2, run_date=run_date)
                    if clean_date:
                        clean_info_date = clean_date
                        original_short_note = line2
                        body_start_idx = 2

    body_text = "\n".join(lines[body_start_idx:])
    return headline, clean_info_date, original_short_note, url_lists, body_text


def extract_telegram_channels(text_str: str) -> list[str]:
    """Find all Telegram channel handles mentioned via t.me/<channel> or @<channel>."""
    handles: list[str] = []
    for m in re.finditer(r"(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,32})", text_str):
        handles.append(m.group(1).lower())
    for m in re.finditer(r"@([a-zA-Z0-9_]{5,32})", text_str):
        handles.append(m.group(1).lower())
    return list(dict.fromkeys(handles))


def clean_polarization_sentence(sentence: str) -> str | None:
    """
    Clean conversational sentence in polarization category:
      - Strip emojis and non-linguistic noise characters.
      - Drop sentence if it contains no Myanmar Unicode script.
      - Drop sentence if token count (syllables or words) < 8.
    """
    if not sentence or not sentence.strip():
        return None

    # 1. Must contain Myanmar Unicode characters
    if not re.search(r"[\u1000-\u109F\uAA60-\uAA7F\uA9E0-\uA9FF]", sentence):
        return None

    # 2. Strip emojis, pictographs, and non-linguistic decorative symbols
    cleaned = re.sub(
        r"[\U00010000-\U0010ffff\u2600-\u27bf\ufe0f\u200d\u25a0-\u25ff\u2b00-\u2bff\u2300-\u23ff]",
        "",
        sentence,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned:
        return None

    # 3. Short sentence filter (< 8 syllables/words)
    syllables = re.findall(
        r"(?:(?<![်္])([က-အ]|[\u1000-\u1021\u1023-\u102A\u1040-\u1049])|[a-zA-Z0-9]+)",
        cleaned,
    )
    words = cleaned.split()
    token_count = max(len(syllables), len(words))
    if token_count < 8:
        return None

    return cleaned


# ===========================================================================
# Date / window utilities
# ===========================================================================

def _to_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_day_windows(
    from_date: date,
    to_date: date,
) -> list[tuple[str, datetime, datetime]]:
    windows = []
    current = from_date
    while current <= to_date:
        day_start = datetime(current.year, current.month, current.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
        windows.append((current.strftime("%Y-%m-%d"), day_start, day_end))
        current += timedelta(days=1)
    return windows


# ===========================================================================
# Database helpers
# ===========================================================================

def get_pg_engine(pg_url: str, schema: str = "public"):
    """Create engine for Neon PostgreSQL configured for target schema."""
    engine = create_engine(pg_url, pool_pre_ping=True)
    if engine.dialect.name == "postgresql":
        if schema and schema != "public":
            with engine.connect() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                conn.commit()
        return engine.execution_options(schema_translate_map={None: schema})
    return engine


def get_latest_cleaned_date(session, channel_name: str) -> date | None:
    result = (
        session.query(func.max(CleaningLog.run_date))
        .filter(
            CleaningLog.channel_name == channel_name,
            CleaningLog.status == "completed",
        )
        .scalar()
    )
    if result is None:
        return None
    return datetime.strptime(result, "%Y-%m-%d").date()


def start_cleaning_log(
    session, channel_name: str, run_date: str, category: str | None = None, force: bool = False
) -> CleaningLog:
    if force:
        session.query(CleaningLog).filter_by(channel_name=channel_name, run_date=run_date).delete()
        session.flush()

    entry = CleaningLog(
        channel_name=channel_name,
        category=category,
        run_date=run_date,
        status="running",
        cleaning_start_ts=datetime.now(timezone.utc),
    )
    session.add(entry)
    session.flush()
    return entry


def finish_cleaning_log(
    session,
    entry: CleaningLog,
    messages_processed: int,
    messages_skipped: int,
    sentences_generated: int,
    status: str = "completed",
) -> None:
    entry.messages_processed = messages_processed
    entry.messages_skipped = messages_skipped
    entry.sentences_generated = sentences_generated
    entry.cleaning_end_ts = datetime.now(timezone.utc)
    entry.status = status
    session.flush()


def channel_day_already_completed(session, channel_name: str, run_date: str) -> bool:
    return (
        session.query(CleaningLog)
        .filter_by(channel_name=channel_name, run_date=run_date, status="completed")
        .count() > 0
    )


def iter_messages_for_channel_window(
    session,
    channel_name: str,
    day_start: datetime,
    day_end: datetime,
) -> Generator[TelegramMessage, None, None]:
    query = (
        session.query(TelegramMessage)
        .filter(
            TelegramMessage.channel_name == channel_name,
            TelegramMessage.date.isnot(None),
            TelegramMessage.date >= day_start,
            TelegramMessage.date <= day_end,
        )
        .order_by(TelegramMessage.id.asc())
    )
    for msg in query:
        if not msg.message_text or not msg.message_text.strip():
            continue
        yield msg


# ===========================================================================
# Staging File Helpers
# ===========================================================================

def stage_channel_records(
    stage_dir: str,
    category: str,
    channel_name: str,
    run_date: str,
    text_rows: list[dict],
    extra_rows: list[dict],
    error_rows: list[dict],
) -> dict:
    """Write intermediate records to stage_dir/<category>/<channel>_<run_date>_*.jsonl files."""
    cat_dir = os.path.join(stage_dir, category)
    os.makedirs(cat_dir, exist_ok=True)
    norm_ch = channel_name.lstrip("@").lower()

    if text_rows:
        text_file = os.path.join(cat_dir, f"{norm_ch}_{run_date}_clean_tele_text.jsonl")
        with open(text_file, "a", encoding="utf-8") as f:
            for r in text_rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    if extra_rows:
        extra_file = os.path.join(cat_dir, f"{norm_ch}_{run_date}_clean_tele_extra_info.jsonl")
        existing_keys: set[tuple[str, int | str]] = set()
        if os.path.exists(extra_file):
            with open(extra_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            item = json.loads(line)
                            k = (item.get("channel_name"), item.get("message_id") or item.get("headline"))
                            existing_keys.add(k)
                        except Exception:
                            pass
        with open(extra_file, "a", encoding="utf-8") as f:
            for r in extra_rows:
                k = (r.get("channel_name"), r.get("message_id") or r.get("headline"))
                if k not in existing_keys:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    existing_keys.add(k)

    if error_rows:
        err_file = os.path.join(cat_dir, f"{norm_ch}_{run_date}_errors.jsonl")
        with open(err_file, "a", encoding="utf-8") as f:
            for r in error_rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "text_staged": len(text_rows),
        "extra_staged": len(extra_rows),
        "errors_staged": len(error_rows),
    }


# ===========================================================================
# Core Processing Engine
# ===========================================================================

def _process_channel_day(
    session,
    channel_name: str,
    run_date: str,
    day_start: datetime,
    day_end: datetime,
    bigrams: list[str] | None,
    dry_run: bool,
    force: bool,
    category: str | None = None,
    stage_dir: str | None = None,
    known_channels: set[str] | None = None,
) -> dict:
    """
    Clean all TelegramMessages for a single channel and calendar day.
    Supports News and Polarization transformations, DLQ error trapping,
    and file-based staging.
    """
    cat_label = category or "general"
    log.info(
        "  Processing [%s] channel: %s for day: %s [%s → %s]",
        cat_label, channel_name, run_date, day_start.isoformat(), day_end.isoformat()
    )

    # Watermark check
    if not dry_run and not force and not stage_dir and channel_day_already_completed(session, channel_name, run_date):
        msg_text = f"    ✅ Watermark found — channel {channel_name} on {run_date} already completed, skipping."
        log.info(msg_text)
        print(msg_text)
        return {"msgs_processed": 0, "msgs_skipped": 0, "sentences_generated": 0, "skipped_day": True}

    cleaning_entry = None
    if stage_dir and force:
        cat_dir = os.path.join(stage_dir, category or "general")
        norm_ch = channel_name.lstrip("@").lower()
        for suffix in ["_clean_tele_text.jsonl", "_clean_tele_extra_info.jsonl", "_errors.jsonl"]:
            old_f = os.path.join(cat_dir, f"{norm_ch}_{run_date}{suffix}")
            if os.path.exists(old_f):
                try:
                    os.remove(old_f)
                except Exception:
                    pass

    if not dry_run and not stage_dir:
        cleaning_entry = start_cleaning_log(session, channel_name, run_date, category=category, force=force)

        # On force: delete existing rows for this window, guarding against deleting human annotations
        if force:
            annotated_subq = session.query(AnnotationResult.clean_line_id).subquery()
            msg_ids_query = session.query(TelegramMessage.id).filter(
                TelegramMessage.channel_name == channel_name,
                TelegramMessage.date.isnot(None),
                TelegramMessage.date >= day_start,
                TelegramMessage.date <= day_end,
            )
            deleted = (
                session.query(CleanTeleText)
                .filter(
                    CleanTeleText.channel_name == channel_name,
                    CleanTeleText.telegram_message_id.in_(msg_ids_query),
                    CleanTeleText.id.not_in(annotated_subq),
                )
                .delete(synchronize_session=False)
            )
            if deleted:
                log.info("  --force: deleted %d unannotated CleanTeleText rows for %s on %s.", deleted, channel_name, run_date)

            src_msg_ids = session.query(TelegramMessage.message_id).filter(
                TelegramMessage.channel_name == channel_name,
                TelegramMessage.date.isnot(None),
                TelegramMessage.date >= day_start,
                TelegramMessage.date <= day_end,
            )
            session.query(CleanTeleExtraInfo).filter(
                CleanTeleExtraInfo.channel_name == channel_name,
                CleanTeleExtraInfo.message_id.in_(src_msg_ids),
            ).delete(synchronize_session=False)

    msgs_processed = 0
    msgs_skipped = 0
    sentences_generated = 0
    staged_texts: list[dict] = []
    staged_extras: list[dict] = []
    staged_errors: list[dict] = []

    seen_hashes: set[str] = set()
    seen_extra_keys: set[tuple[str, int | str]] = set()

    for msg in iter_messages_for_channel_window(session, channel_name, day_start, day_end):
        try:
            # Intra-channel same-day deduplication: guarantee 1 row per unique raw text across categories
            raw_text_clean = msg.message_text.strip() if msg.message_text else ""
            if not raw_text_clean:
                msgs_skipped += 1
                continue

            msg_hash = hashlib.sha256(raw_text_clean.encode("utf-8")).hexdigest()
            if msg_hash in seen_hashes:
                msgs_skipped += 1
                continue
            seen_hashes.add(msg_hash)

            # ---------------------------------------------------------------
            # 1. Polarization Category Pipeline
            # ---------------------------------------------------------------
            if cat_label == "polarization":
                # Channel discovery logging
                discovered = extract_telegram_channels(msg.message_text)
                for d_ch in discovered:
                    if known_channels and d_ch not in known_channels:
                        disc_msg = f"[DISCOVERY] Found unmonitored Telegram channel '@{d_ch}' in channel '{channel_name}' (msg_id: {msg.message_id})"
                        log.warning(disc_msg)
                        print(f"      🔍 {disc_msg}")

                # Split sentences
                raw_sentences = split_myanmar_sentences(msg.message_text, bigrams=bigrams)
                valid_sentences: list[str] = []
                for s in raw_sentences:
                    cleaned_s = clean_polarization_sentence(s)
                    if cleaned_s:
                        valid_sentences.append(cleaned_s)

                if not valid_sentences:
                    msgs_skipped += 1
                    continue

                msgs_processed += 1
                for idx, sent in enumerate(valid_sentences):
                    record = {
                        "telegram_message_id": msg.id,
                        "line_index": idx,
                        "sentence": sent,
                        "channel_name": msg.channel_name,
                        "category": category,
                        "source_message_id": msg.message_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if stage_dir:
                        staged_texts.append(record)
                    elif not dry_run:
                        session.add(CleanTeleText(
                            telegram_message_id=msg.id,
                            line_index=idx,
                            sentence=sent,
                            channel_name=msg.channel_name,
                            category=category,
                            source_message_id=msg.message_id,
                        ))
                sentences_generated += len(valid_sentences)

            # ---------------------------------------------------------------
            # 2. News Category Pipeline
            # ---------------------------------------------------------------
            elif cat_label == "news":
                headline, clean_info_date, original_short_note, url_lists, body_text = parse_news_message(msg.message_text, run_date=run_date)

                # Sentences: Headline is included at line_index = 0
                body_sentences = split_myanmar_sentences(body_text, bigrams=bigrams)
                total_sentences: list[tuple[int, str]] = []
                if headline:
                    total_sentences.append((0, headline))
                for i, b_sent in enumerate(body_sentences, start=1):
                    total_sentences.append((i, b_sent))

                if not total_sentences:
                    msgs_skipped += 1
                    continue

                # Ensure exactly one extra info row per raw news text / message
                extra_key = (msg.channel_name, msg.message_id or headline)
                if extra_key not in seen_extra_keys:
                    seen_extra_keys.add(extra_key)
                    extra_record = {
                        "channel_name": msg.channel_name,
                        "category": category or "news",
                        "message_id": msg.message_id,
                        "headline": headline,
                        "clean_info_date": clean_info_date,
                        "original_short_note": original_short_note,
                        "url_lists": json.dumps(url_lists, ensure_ascii=False) if url_lists else None,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if stage_dir:
                        staged_extras.append(extra_record)
                    elif not dry_run:
                        exists = session.query(CleanTeleExtraInfo.id).filter(
                            CleanTeleExtraInfo.channel_name == msg.channel_name,
                            CleanTeleExtraInfo.message_id == msg.message_id,
                        ).first()
                        if not exists:
                            session.add(CleanTeleExtraInfo(
                                channel_name=msg.channel_name,
                                category=category or "news",
                                message_id=msg.message_id,
                                headline=headline,
                                clean_info_date=clean_info_date,
                                original_short_note=original_short_note,
                                url_lists=json.dumps(url_lists, ensure_ascii=False) if url_lists else None,
                            ))

                msgs_processed += 1
                for l_idx, sent in total_sentences:
                    text_record = {
                        "telegram_message_id": msg.id,
                        "line_index": l_idx,
                        "sentence": sent,
                        "channel_name": msg.channel_name,
                        "category": category,
                        "source_message_id": msg.message_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if stage_dir:
                        staged_texts.append(text_record)
                    elif not dry_run:
                        session.add(CleanTeleText(
                            telegram_message_id=msg.id,
                            line_index=l_idx,
                            sentence=sent,
                            channel_name=msg.channel_name,
                            category=category,
                            source_message_id=msg.message_id,
                        ))
                sentences_generated += len(total_sentences)

            # ---------------------------------------------------------------
            # 3. Default General Pipeline
            # ---------------------------------------------------------------
            else:
                sentences = split_myanmar_sentences(msg.message_text, bigrams=bigrams)
                if not sentences:
                    msgs_skipped += 1
                    continue

                msgs_processed += 1
                for idx, sent in enumerate(sentences):
                    record = {
                        "telegram_message_id": msg.id,
                        "line_index": idx,
                        "sentence": sent,
                        "channel_name": msg.channel_name,
                        "category": category,
                        "source_message_id": msg.message_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if stage_dir:
                        staged_texts.append(record)
                    elif not dry_run:
                        session.add(CleanTeleText(
                            telegram_message_id=msg.id,
                            line_index=idx,
                            sentence=sent,
                            channel_name=msg.channel_name,
                            category=category,
                            source_message_id=msg.message_id,
                        ))
                sentences_generated += len(sentences)

        except Exception as exc:
            err_type = type(exc).__name__
            err_msg = str(exc)
            stack = traceback.format_exc()
            log.error("Error processing msg_id=%s in channel=%s: %s", msg.id, channel_name, err_msg)
            err_record = {
                "channel_name": msg.channel_name,
                "category": category or "general",
                "run_date": run_date,
                "telegram_message_id": msg.id,
                "source_message_id": msg.message_id,
                "raw_text": msg.message_text,
                "error_type": err_type,
                "error_message": err_msg,
                "stack_trace": stack,
                "retry_count": 0,
                "resolved": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            if stage_dir:
                staged_errors.append(err_record)
            elif not dry_run:
                session.add(CleaningErrorLog(
                    channel_name=msg.channel_name,
                    category=category or "general",
                    run_date=run_date,
                    telegram_message_id=msg.id,
                    source_message_id=msg.message_id,
                    raw_text=msg.message_text,
                    error_type=err_type,
                    error_message=err_msg,
                    stack_trace=stack,
                    retry_count=0,
                    resolved=False,
                ))

    # Commit or flush stage
    if stage_dir:
        stage_channel_records(
            stage_dir=stage_dir,
            category=category or "general",
            channel_name=channel_name,
            run_date=run_date,
            text_rows=staged_texts,
            extra_rows=staged_extras,
            error_rows=staged_errors,
        )
    elif not dry_run:
        session.commit()
        if cleaning_entry:
            finish_cleaning_log(
                session, cleaning_entry,
                messages_processed=msgs_processed,
                messages_skipped=msgs_skipped,
                sentences_generated=sentences_generated,
            )
            session.commit()

    return {
        "msgs_processed": msgs_processed,
        "msgs_skipped": msgs_skipped,
        "sentences_generated": sentences_generated,
        "skipped_day": False,
    }


# ===========================================================================
# Bulk Ingestion Engine (--upload-staged)
# ===========================================================================

def upload_staged_data(config: dict, stage_dir: str, category: str, env: str | None = None) -> dict:
    """
    Ingest all staged .jsonl files for category from stage_dir into Neon PostgreSQL
    in a single pooled connection and transaction.
    """
    environment, schema = resolve_environment_and_schema(config, env_override=env)
    pg_url = config.get("postgresql", {}).get("url") or os.getenv("NEON_DATABASE_URL")
    if not pg_url:
        raise ValueError("NEON_DATABASE_URL or postgresql.url not configured.")

    cat_path = Path(stage_dir) / category
    if not cat_path.exists():
        print(f"[*] Staging path does not exist: {cat_path}. Nothing to upload.")
        return {"uploaded_texts": 0, "uploaded_extras": 0, "uploaded_errors": 0}

    engine = get_pg_engine(pg_url, schema=schema)
    AnnotationBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("=" * 65)
    print(f" 🚀 Bulk Ingesting Staged Clean Data: category='{category}'")
    print(f" Staging Directory: {cat_path}")
    print(f" Target Schema    : {schema}")
    print("=" * 65)

    uploaded_texts = 0
    uploaded_extras = 0
    uploaded_errors = 0
    files_to_delete: list[Path] = []
    channel_dates: dict[tuple[str, str], dict] = {}

    try:
        # Ingest clean_tele_text
        text_files = list(cat_path.glob("*_clean_tele_text.jsonl"))
        for tf in text_files:
            batch: list[dict] = []
            with open(tf, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    ch = item.get("channel_name")
                    # Parse run_date from filename or created_at
                    parts = tf.stem.split("_")
                    r_date = parts[1] if len(parts) >= 3 else item.get("created_at", "")[:10]
                    key = (ch, r_date)
                    if key not in channel_dates:
                        channel_dates[key] = {"sents": 0, "msgs": set()}
                    channel_dates[key]["sents"] += 1
                    if item.get("telegram_message_id"):
                        channel_dates[key]["msgs"].add(item["telegram_message_id"])

                    # Reformat created_at
                    if isinstance(item.get("created_at"), str):
                        try:
                            item["created_at"] = datetime.fromisoformat(item["created_at"])
                        except Exception:
                            item["created_at"] = datetime.now(timezone.utc)
                    batch.append(item)
                    if len(batch) >= 1000:
                        session.bulk_insert_mappings(CleanTeleText, batch)
                        session.flush()
                        uploaded_texts += len(batch)
                        batch = []
            if batch:
                session.bulk_insert_mappings(CleanTeleText, batch)
                session.flush()
                uploaded_texts += len(batch)
            files_to_delete.append(tf)

        # Ingest clean_tele_extra_info with strict deduplication
        extra_files = list(cat_path.glob("*_clean_tele_extra_info.jsonl"))
        if extra_files:
            # Load existing (channel_name, message_id) to guarantee idempotency and uniqueness
            existing_extra_keys: set[tuple[str, int | str]] = set()
            try:
                db_extras = session.query(CleanTeleExtraInfo.channel_name, CleanTeleExtraInfo.message_id).filter(
                    CleanTeleExtraInfo.category == category
                ).all()
                existing_extra_keys = {(ch, mid) for ch, mid in db_extras if ch and mid is not None}
            except Exception as e:
                log.warning("Could not pre-fetch existing extra keys: %s", e)

            seen_extra_keys: set[tuple[str, int | str]] = set()

            for ef in extra_files:
                batch = []
                with open(ef, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        item = json.loads(line)
                        ch = item.get("channel_name")
                        msg_id = item.get("message_id")
                        key = (ch, msg_id or item.get("headline"))
                        if key in existing_extra_keys or key in seen_extra_keys:
                            continue
                        seen_extra_keys.add(key)
                        existing_extra_keys.add(key)

                        if isinstance(item.get("created_at"), str):
                            try:
                                item["created_at"] = datetime.fromisoformat(item["created_at"])
                            except Exception:
                                item["created_at"] = datetime.now(timezone.utc)
                        batch.append(item)
                        if len(batch) >= 1000:
                            session.bulk_insert_mappings(CleanTeleExtraInfo, batch)
                            session.flush()
                            uploaded_extras += len(batch)
                            batch = []
                if batch:
                    session.bulk_insert_mappings(CleanTeleExtraInfo, batch)
                    session.flush()
                    uploaded_extras += len(batch)
                files_to_delete.append(ef)

        # Ingest errors
        err_files = list(cat_path.glob("*_errors.jsonl"))
        for erf in err_files:
            batch = []
            with open(erf, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    if isinstance(item.get("created_at"), str):
                        try:
                            item["created_at"] = datetime.fromisoformat(item["created_at"])
                        except Exception:
                            item["created_at"] = datetime.now(timezone.utc)
                    batch.append(item)
                    if len(batch) >= 1000:
                        session.bulk_insert_mappings(CleaningErrorLog, batch)
                        session.flush()
                        uploaded_errors += len(batch)
                        batch = []
            if batch:
                session.bulk_insert_mappings(CleaningErrorLog, batch)
                session.flush()
                uploaded_errors += len(batch)
            files_to_delete.append(erf)

        # Update CleaningLog watermarks
        for (ch, r_date), stats in channel_dates.items():
            if not ch or not r_date:
                continue
            log_row = session.query(CleaningLog).filter_by(channel_name=ch, run_date=r_date).first()
            if not log_row:
                log_row = CleaningLog(
                    channel_name=ch,
                    category=category,
                    run_date=r_date,
                    status="completed",
                    messages_processed=len(stats["msgs"]),
                    messages_skipped=0,
                    sentences_generated=stats["sents"],
                    cleaning_start_ts=datetime.now(timezone.utc),
                    cleaning_end_ts=datetime.now(timezone.utc),
                )
                session.add(log_row)
            else:
                log_row.status = "completed"
                log_row.category = category
                log_row.sentences_generated += stats["sents"]
                log_row.messages_processed += len(stats["msgs"])
                log_row.cleaning_end_ts = datetime.now(timezone.utc)

        session.commit()
        print(f"  ✔ Ingestion committed: {uploaded_texts} sentences, {uploaded_extras} extras, {uploaded_errors} errors.")

        # Cleanup files after verified commit
        for p in files_to_delete:
            try:
                p.unlink()
            except OSError as err:
                log.warning("Could not unlink staging file %s: %s", p, err)

    finally:
        session.close()

    return {
        "uploaded_texts": uploaded_texts,
        "uploaded_extras": uploaded_extras,
        "uploaded_errors": uploaded_errors,
    }


# ===========================================================================
# DLQ Replay Engine (--retry-dlq)
# ===========================================================================

def retry_dlq(
    config: dict,
    category: str | None = None,
    channel: str | None = None,
    dict_path: str | None = None,
    env: str | None = None,
) -> dict:
    """Retry unresolved errors in cleaning_error_logs."""
    environment, schema = resolve_environment_and_schema(config, env_override=env)
    pg_url = config.get("postgresql", {}).get("url") or os.getenv("NEON_DATABASE_URL")
    if not pg_url:
        raise ValueError("NEON_DATABASE_URL or postgresql.url not configured.")

    engine = get_pg_engine(pg_url, schema=schema)
    AnnotationBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("=" * 65)
    print(" 🔁 Retrying Dead Letter Queue (cleaning_error_logs)")
    print(f" Environment : {environment} (schema: {schema})")
    print(f" Category    : {category or 'all'}")
    print(f" Channel     : {channel or 'all'}")
    print("=" * 65)

    bigrams = None
    if dict_path and os.path.exists(dict_path):
        bigrams = _load_dict_file(dict_path)

    query = session.query(CleaningErrorLog).filter(CleaningErrorLog.resolved == False)
    if category:
        query = query.filter(CleaningErrorLog.category == category)
    if channel:
        query = query.filter(CleaningErrorLog.channel_name == channel)

    errors = query.all()
    print(f"[*] Found {len(errors)} unresolved DLQ record(s).")
    recovered = 0
    failed = 0

    try:
        for err in errors:
            if not err.raw_text:
                err.resolved = True
                err.resolved_at = datetime.now(timezone.utc)
                continue

            try:
                cat = err.category
                sentences: list[str] = []
                if cat == "polarization":
                    raw_sents = split_myanmar_sentences(err.raw_text, bigrams=bigrams)
                    for s in raw_sents:
                        cs = clean_polarization_sentence(s)
                        if cs:
                            sentences.append(cs)
                elif cat == "news":
                    headline, clean_info_date, original_short_note, urls, body = parse_news_message(err.raw_text, run_date=err.run_date)
                    if headline:
                        sentences.append(headline)
                    sentences.extend(split_myanmar_sentences(body, bigrams=bigrams))
                else:
                    sentences = split_myanmar_sentences(err.raw_text, bigrams=bigrams)

                if sentences:
                    for i, sent in enumerate(sentences):
                        session.add(CleanTeleText(
                            telegram_message_id=err.telegram_message_id,
                            line_index=i,
                            sentence=sent,
                            channel_name=err.channel_name,
                            category=err.category,
                            source_message_id=err.source_message_id,
                        ))
                err.resolved = True
                err.resolved_at = datetime.now(timezone.utc)
                recovered += 1
            except Exception as retry_exc:
                err.retry_count += 1
                err.error_message = f"Retry failed: {retry_exc}"
                failed += 1

        session.commit()
        print(f"  ✔ DLQ Replay Complete: Recovered={recovered}, Failed={failed}")
    finally:
        session.close()

    return {"recovered": recovered, "failed": failed}


# ===========================================================================
# Main Pipeline Entry
# ===========================================================================

def clean_and_upload(
    config: dict,
    channel: str | None = None,
    dry_run: bool = False,
    force: bool = False,
    dict_path: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    env: str | None = None,
    category: str | None = None,
    stage_dir: str | None = None,
) -> None:
    environment, schema = resolve_environment_and_schema(config, env_override=env)

    effective_dict = dict_path or os.getenv("CLEANER_DICT_PATH") or config.get("cleaner", {}).get("dict_path")
    if effective_dict:
        if not os.path.isabs(effective_dict) and not os.path.exists(effective_dict):
            cand_root = os.path.join(PROJECT_ROOT, effective_dict)
            cand_base = os.path.join(BASE_DIR, effective_dict)
            if os.path.exists(cand_root):
                effective_dict = cand_root
            elif os.path.exists(cand_base):
                effective_dict = cand_base

        log.info("Loading custom bigram dict: %s", effective_dict)
        print(f"[*] Loading bigram dictionary from: {effective_dict}")
        bigrams: list[str] | None = _load_dict_file(effective_dict)
    else:
        bigrams = None

    pg_url = config.get("postgresql", {}).get("url") or os.getenv("NEON_DATABASE_URL", "")
    if not pg_url:
        raise ValueError("PostgreSQL URL not set. Provide NEON_DATABASE_URL or postgresql.url in config.")

    if not dry_run and not stage_dir:
        init_annotation_db(pg_url, schema=schema, config=config)

    engine = get_pg_engine(pg_url, schema=schema)
    Session = sessionmaker(bind=engine)
    session = Session()

    known_channels = get_known_channels(config)

    try:
        if channel:
            channels = [str(channel)]
        else:
            from services.telegram_scraper.scraper import resolve_channels_for_category
            resolved_chs, _ = resolve_channels_for_category(config, category=category)
            channels = [str(ch) for ch in resolved_chs] if resolved_chs else []
            if not channels:
                db_channels = session.query(TelegramMessage.channel_name).distinct().all()
                channels = [ch[0] for ch in db_channels if ch[0]]

        if not channels:
            log.info("No channels configured or found in database. Nothing to clean.")
            print("No channels configured or found in database. Nothing to clean.")
            return

        today_utc = datetime.now(timezone.utc).date()
        resolved_to: date = _to_date(to_date) or today_utc

        print("=" * 65)
        print("  🧹 Myanmar Sentence Cleaner (Concurreny & Advanced Cleaning)")
        print("=" * 65)
        print(f"  Environment : {environment} (schema: {schema})")
        print(f"  Category    : {category or 'all'}")
        print(f"  Channels    : {channels}")
        print(f"  To Date     : {resolved_to}")
        print(f"  Staging Dir : {stage_dir or 'Disabled (Direct DB Write)'}")
        print(f"  Dry-run     : {dry_run}")
        print(f"  Force       : {force}")
        print("=" * 65)

        grand_msgs = 0
        grand_skipped = 0
        grand_sents = 0
        total_days_run = 0
        total_days_skip = 0

        for ch in channels:
            print(f"\n📢  Channel: {ch}")
            log.info("Starting cleaning for channel: %s", ch)

            resolved_from: date
            if from_date:
                resolved_from = _to_date(from_date)
            else:
                latest = get_latest_cleaned_date(session, ch)
                if latest is None:
                    earliest = (
                        session.query(func.min(TelegramMessage.date))
                        .filter(TelegramMessage.channel_name == ch)
                        .scalar()
                    )
                    if earliest is None:
                        print(f"  ℹ️  No TelegramMessage rows found for {ch}. Skipping channel.")
                        log.info("No messages for channel %s.", ch)
                        continue
                    resolved_from = earliest.date() if hasattr(earliest, "date") else earliest
                else:
                    resolved_from = latest + timedelta(days=1)

            if resolved_from > resolved_to:
                print(f"  ✅ Channel {ch} already up-to-date (latest completed: {resolved_from - timedelta(days=1)}).")
                continue

            day_windows = build_day_windows(resolved_from, resolved_to)
            print(f"  Date range  : {resolved_from} → {resolved_to}  ({len(day_windows)} day(s))")

            ch_msgs = 0
            ch_skipped = 0
            ch_sents = 0

            for run_date, day_start, day_end in day_windows:
                stats = _process_channel_day(
                    session=session,
                    channel_name=ch,
                    run_date=run_date,
                    day_start=day_start,
                    day_end=day_end,
                    bigrams=bigrams,
                    dry_run=dry_run,
                    force=force,
                    category=category,
                    stage_dir=stage_dir,
                    known_channels=known_channels,
                )

                if stats.get("skipped_day"):
                    total_days_skip += 1
                    continue

                total_days_run += 1
                ch_msgs += stats["msgs_processed"]
                ch_skipped += stats["msgs_skipped"]
                ch_sents += stats["sentences_generated"]

                tag = "[STAGE] " if stage_dir else ("[DRY-RUN] " if dry_run else "")
                print(
                    f"    {tag}✔  {run_date}: "
                    f"msgs_processed={stats['msgs_processed']}  "
                    f"msgs_skipped={stats['msgs_skipped']}  "
                    f"sentences={stats['sentences_generated']}"
                )

            grand_msgs += ch_msgs
            grand_skipped += ch_skipped
            grand_sents += ch_sents

        print()
        print("=" * 65)
        print("  Summary")
        print("=" * 65)
        print(f"  Channels evaluated : {len(channels)}")
        print(f"  Channel-days run   : {total_days_run}")
        print(f"  Channel-days skip  : {total_days_skip}")
        print(f"  Msgs processed     : {grand_msgs}")
        print(f"  Msgs skipped       : {grand_skipped}")
        print(f"  Sentences generated: {grand_sents}")
        print("=" * 65)

    finally:
        session.close()


# ===========================================================================
# CLI entry point
# ===========================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Myanmar sentence cleaner & advanced data transformation pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default="config.yaml", help="Config file path.")
    parser.add_argument("--env", choices=["dev", "prod"], default=None, help="Target environment.")
    parser.add_argument("--channel", "-c", default=None, metavar="NAME", help="Specific channel name.")
    parser.add_argument("--category", default=None, help="Category ('polarization' or 'news').")
    parser.add_argument("--dry-run", action="store_true", help="Preview splits without writing.")
    parser.add_argument("--force", action="store_true", help="Re-process already completed days.")
    parser.add_argument("--dict", default=None, metavar="PATH", help="Custom bigram dict file.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging.")

    # Staging & Bulk Ingestion flags
    parser.add_argument("--stage-dir", default=None, help="Local staging directory for file-based outputs.")
    parser.add_argument("--upload-staged", action="store_true", help="Bulk upload staged .jsonl files for category.")

    # DLQ retry flag
    parser.add_argument("--retry-dlq", action="store_true", help="Retry failed texts in cleaning_error_logs.")

    # Date range
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--yesterday", action="store_true", help="Clean only yesterday's 24h day window.")
    date_group.add_argument("--lookback", type=int, default=None, metavar="DAYS", help="Lookback days.")
    date_group.add_argument("--from-date", metavar="YYYY-MM-DD", default=None, help="Start date.")
    parser.add_argument("--to-date", metavar="YYYY-MM-DD", default=None, help="End date.")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = load_config(args.config)

    # 1. Bulk Upload Mode
    if args.upload_staged:
        if not args.category:
            print("❌ Error: --upload-staged requires --category (e.g. 'polarization' or 'news').", file=sys.stderr)
            sys.exit(1)
        if not args.stage_dir:
            print("❌ Error: --upload-staged requires --stage-dir.", file=sys.stderr)
            sys.exit(1)
        upload_staged_data(cfg, stage_dir=args.stage_dir, category=args.category, env=args.env)
        sys.exit(0)

    # 2. DLQ Replay Mode
    if args.retry_dlq:
        retry_dlq(cfg, category=args.category, channel=args.channel, dict_path=args.dict, env=args.env)
        sys.exit(0)

    # 3. Normal / Staging Cleaning Mode
    resolved_from_date = args.from_date
    resolved_to_date = args.to_date

    if args.yesterday:
        yesterday_str = (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")
        resolved_from_date = yesterday_str
        resolved_to_date = yesterday_str
    elif args.lookback is not None:
        if args.lookback < 1:
            print("error: --lookback must be at least 1", file=sys.stderr)
            sys.exit(1)
        lookback_start = datetime.now(timezone.utc).date() - timedelta(days=args.lookback - 1)
        resolved_from_date = lookback_start.strftime("%Y-%m-%d")

    clean_and_upload(
        config=cfg,
        channel=args.channel,
        dry_run=args.dry_run,
        force=args.force,
        dict_path=args.dict,
        from_date=resolved_from_date,
        to_date=resolved_to_date,
        env=args.env,
        category=args.category,
        stage_dir=args.stage_dir,
    )
