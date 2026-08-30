"""
cleaner.py — Myanmar Sentence Cleaner & Annotation DB Uploader
===============================================================
Pipeline:
  1. Read TelegramMessage rows from Neon PostgreSQL (scraper DB) for a given
     channel and day window (filtered by TelegramMessage.channel_name and date).
  2. Split each message's text into individual Myanmar sentences using a
     regex-based algorithm ported from Dr. Ye Kyaw Thu's my-linebreak.pl
     (https://github.com/ye-kyaw-thu/tools/blob/master/perl/my-linebreak.pl).
  3. Write CleanTeleText rows to Neon PostgreSQL (annotation tables).
     Both read and write use the same Neon PostgreSQL connection string
     (NEON_DATABASE_URL / config postgresql.url).
  4. Record a CleaningLog watermark row per channel and calendar day so
     subsequent runs are idempotent and can be backfilled for any date range.

Imports
-------
  shared.scraper_models    → TelegramMessage  (read source)
  shared.annotation_models → CleanTeleText, CleaningLog  (write destination)

Usage
-----
    # Default incremental: cleans all channels after their latest watermark date
    uv run python services/telegram_scraper/cleaner.py

    # Clean a single channel incrementally
    uv run python services/telegram_scraper/cleaner.py --channel shweba000

    # Dry-run preview
    uv run python services/telegram_scraper/cleaner.py --dry-run

    # Backfill a specific date range (re-runs even if already completed)
    uv run python services/telegram_scraper/cleaner.py --from-date 2025-08-01 --to-date 2025-08-07 --force

    # Use a custom bigram dictionary
    uv run python services/telegram_scraper/cleaner.py --dict ref/1syl.potma.dict
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import logging
import unicodedata
from datetime import date, datetime, timedelta, timezone
from typing import Generator

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "../../"))
sys.path.insert(0, PROJECT_ROOT)

# Scraper models — read TelegramMessage rows
from shared.scraper_models import TelegramMessage

# Annotation models — write CleanTeleText, CleaningLog rows
from shared.annotation_models import CleanTeleText, CleaningLog, init_annotation_db

load_dotenv()

logging.basicConfig(
    filename='output.log',
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ===========================================================================
# Config loader
# ===========================================================================

def load_config(config_file: str = "config.yaml") -> dict:
    target = os.path.join(BASE_DIR, config_file)
    if not os.path.exists(target):
        target = os.path.join(BASE_DIR, "config.example.yaml")
    with open(target, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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
    """
    Return True if *segment* qualifies as a real sentence.

    A segment is rejected when every character is a non-letter, non-digit
    Unicode character — i.e. the segment is composed entirely of symbols,
    punctuation, separators, or ASCII decoration (e.g. "=====", "-----",
    "*****", "▬▬▬").  At least one character must have a Unicode General
    Category that starts with 'L' (letter) or 'N' (number) for the
    segment to be accepted.
    """
    return any(
        unicodedata.category(ch)[0] in {"L", "N"}
        for ch in segment
    )


def split_myanmar_sentences(
    paragraph: str,
    bigrams: list[str] | None = None,
) -> list[str]:
    r"""
    Split a Myanmar paragraph into individual sentences.

    Algorithm (my-linebreak.pl by Dr.Ye Kyaw Thu):
      1. Process each line independently.
      2. Strip leading/trailing whitespace.       → Perl: s/^\s+|\s+$//g
      3. Collapse multiple spaces.                → Perl: s/ +/ /g
      4. Insert \n after every matched bigram.   → Perl: s/($re)/$1\n/g
      5. Split on newlines, discard empty.
    """
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
# Date / window utilities
# ===========================================================================

def _to_date(value: str | date | None) -> date | None:
    """Parse a YYYY-MM-DD string to a date object (passthrough if already date)."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_day_windows(
    from_date: date,
    to_date: date,
) -> list[tuple[str, datetime, datetime]]:
    """
    Return a list of (run_date_str, day_start_utc, day_end_utc) tuples
    covering [from_date, to_date] inclusive, ordered chronologically.
    """
    windows = []
    current = from_date
    while current <= to_date:
        day_start = datetime(current.year, current.month, current.day,
                             0, 0, 0, tzinfo=timezone.utc)
        day_end   = day_start + timedelta(days=1) - timedelta(seconds=1)
        windows.append((current.strftime("%Y-%m-%d"), day_start, day_end))
        current += timedelta(days=1)
    return windows


# ===========================================================================
# Database helpers
# ===========================================================================

def get_pg_engine(pg_url: str):
    """Create engine for Neon PostgreSQL (used for both read and write)."""
    return create_engine(pg_url, pool_pre_ping=True)


def get_latest_cleaned_date(session, channel_name: str) -> date | None:
    """
    Return the most recent run_date that has a 'completed' CleaningLog entry
    for the given channel, or None if no cleaning has been done yet.
    """
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


def start_cleaning_log(session, channel_name: str, run_date: str, force: bool = False) -> CleaningLog:
    """
    Insert a 'running' CleaningLog row for the given channel_name and run_date.
    If force=True and a row already exists, it is deleted first.
    """
    if force:
        session.query(CleaningLog).filter_by(channel_name=channel_name, run_date=run_date).delete()
        session.flush()

    entry = CleaningLog(
        channel_name=channel_name,
        run_date=run_date,
        status="running",
        cleaning_start_ts=datetime.now(datetime.UTC),
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
    """Update a CleaningLog row with final stats and mark it completed/failed."""
    entry.messages_processed  = messages_processed
    entry.messages_skipped    = messages_skipped
    entry.sentences_generated = sentences_generated
    entry.cleaning_end_ts     = datetime.now(datetime.UTC),
    entry.status              = status
    session.flush()


def channel_day_already_completed(session, channel_name: str, run_date: str) -> bool:
    """Return True if a 'completed' CleaningLog row exists for (channel_name, run_date)."""
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
    """
    Yield TelegramMessage rows for a specific channel whose date falls within [day_start, day_end].
    Messages with NULL dates are skipped (they have no calendar day).
    Only messages with non-empty text are yielded.
    """
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
# Core pipeline
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
) -> dict:
    """
    Clean all TelegramMessages for a single channel and calendar day.

    Returns a stats dict: {msgs_processed, msgs_skipped, sentences_generated, skipped_day}.
    """
    log.info("  Processing channel: %s for day: %s [%s → %s]", channel_name, run_date, day_start.isoformat(), day_end.isoformat())

    # --- Watermark check (skip if already done and not forcing) ---
    if not dry_run and not force and channel_day_already_completed(session, channel_name, run_date):
        msg_text = f"    ✅ Watermark found — channel {channel_name} on {run_date} already completed, skipping."
        log.info(msg_text)
        print(msg_text)
        return {"msgs_processed": 0, "msgs_skipped": 0, "sentences_generated": 0, "skipped_day": True}

    # --- Start log row ---
    cleaning_entry = None
    if not dry_run:
        cleaning_entry = start_cleaning_log(session, channel_name, run_date, force=force)

        # On force: delete all existing CleanTeleText rows for this channel in this window
        if force:
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
                )
                .delete(synchronize_session=False)
            )
            if deleted:
                log.info("  --force: deleted %d existing CleanTeleText rows for channel %s on %s.", deleted, channel_name, run_date)

    # --- Process messages ---
    msgs_processed  = 0
    msgs_skipped    = 0
    sentences_generated = 0

    for msg in iter_messages_for_channel_window(session, channel_name, day_start, day_end):
        sentences = split_myanmar_sentences(msg.message_text, bigrams=bigrams)

        if not sentences:
            msgs_skipped += 1
            log.debug("Message id=%d (channel=%s) produced no sentences — skipped.", msg.id, channel_name)
            continue

        msgs_processed += 1

        if dry_run:
            log.info(
                "  [DRY-RUN] msg id=%d channel=%s → %d sentence(s)",
                msg.id, msg.channel_name, len(sentences),
            )
            for i, s in enumerate(sentences[:5]):
                log.info("    [%d] %s", i, s)
        else:
            for i, sent in enumerate(sentences):
                session.add(CleanTeleText(
                    telegram_message_id=msg.id,
                    line_index=i,
                    sentence=sent,
                    channel_name=msg.channel_name,
                    source_message_id=msg.message_id,
                ))
            sentences_generated += len(sentences)

    # --- Commit & finish log ---
    if not dry_run:
        session.commit()
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


def clean_and_upload(
    config: dict,
    channel: str | None = None,
    dry_run: bool = False,
    force: bool = False,
    dict_path: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> None:
    """
    Main pipeline: read TelegramMessage → split → write CleanTeleText.

    Args:
        config:     Loaded YAML config dict.
        channel:    Optional channel name to restrict cleaning to.
        dry_run:    Preview splits without writing to PostgreSQL.
        force:      Re-process days even if they have a 'completed' watermark.
        dict_path:  Path to a custom bigram dictionary file.
        from_date:  YYYY-MM-DD start of the day range to clean (inclusive).
                    Defaults to the day after the latest completed watermark per channel.
        to_date:    YYYY-MM-DD end of the day range to clean (inclusive).
                    Defaults to today (UTC).
    """
    # ------------------------------------------------------------------
    # Bigram dictionary
    # ------------------------------------------------------------------
    effective_dict = dict_path or config.get("cleaner", {}).get("dict_path")
    if effective_dict:
        log.info("Loading custom bigram dict: %s", effective_dict)
        bigrams: list[str] | None = _load_dict_file(effective_dict)
        log.info("  → %d bigrams loaded.", len(bigrams))
    else:
        log.info("Using built-in Myanmar bigram list (%d entries).", len(_DEFAULT_BIGRAMS))
        bigrams = None

    # ------------------------------------------------------------------
    # PostgreSQL connection
    # ------------------------------------------------------------------
    pg_url = (
        os.getenv("NEON_DATABASE_URL")
        or config.get("postgresql", {}).get("url", "")
    )
    if not pg_url:
        raise ValueError(
            "PostgreSQL URL not set. Provide NEON_DATABASE_URL env var "
            "or postgresql.url in config.yaml."
        )

    # Ensure annotation tables exist (incl. cleaning_logs)
    if not dry_run:
        init_annotation_db(pg_url)

    engine = get_pg_engine(pg_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # ------------------------------------------------------------------
        # Resolve target channels
        # ------------------------------------------------------------------
        if channel:
            channels = [str(channel)]
        else:
            channels = [str(ch) for ch in config.get("scraping", {}).get("channels", [])]
            if not channels:
                db_channels = session.query(TelegramMessage.channel_name).distinct().all()
                channels = [ch[0] for ch in db_channels if ch[0]]

        if not channels:
            log.info("No channels configured or found in database. Nothing to clean.")
            print("No channels configured or found in database. Nothing to clean.")
            return

        today_utc = datetime.now(timezone.utc).date()
        resolved_to: date = _to_date(to_date) or today_utc

        # ------------------------------------------------------------------
        # Print run header
        # ------------------------------------------------------------------
        print("=" * 65)
        print("  🧹 Myanmar Sentence Cleaner (Per-Channel Watermark)")
        print("=" * 65)
        print(f"  Channels    : {channels}")
        print(f"  To Date     : {resolved_to}")
        print(f"  Dry-run     : {dry_run}")
        print(f"  Force       : {force}")
        print(f"  Bigram dict : {'custom' if effective_dict else 'built-in'}")
        print("=" * 65)

        grand_msgs      = 0
        grand_skipped   = 0
        grand_sents     = 0
        total_days_run  = 0
        total_days_skip = 0

        # ------------------------------------------------------------------
        # Process each channel
        # ------------------------------------------------------------------
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
                    log.info("Channel %s: starting from earliest message date: %s", ch, resolved_from)
                else:
                    resolved_from = latest + timedelta(days=1)
                    log.info("Channel %s incremental mode: latest completed = %s. Cleaning from %s.", ch, latest, resolved_from)

            if resolved_from > resolved_to:
                print(f"  ✅ Channel {ch} already up-to-date (latest completed: {resolved_from - timedelta(days=1)}).")
                log.info("Channel %s is up-to-date (from=%s > to=%s).", ch, resolved_from, resolved_to)
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
                )

                if stats.get("skipped_day"):
                    total_days_skip += 1
                    continue

                total_days_run  += 1
                ch_msgs         += stats["msgs_processed"]
                ch_skipped      += stats["msgs_skipped"]
                ch_sents        += stats["sentences_generated"]

                tag = "[DRY-RUN] " if dry_run else ""
                print(
                    f"    {tag}✔  {run_date}: "
                    f"msgs_processed={stats['msgs_processed']}  "
                    f"msgs_skipped={stats['msgs_skipped']}  "
                    f"sentences={stats['sentences_generated']}"
                )
                log.info(
                    "%s[%s] %s: processed=%d skipped=%d sentences=%d",
                    tag, ch, run_date,
                    stats["msgs_processed"], stats["msgs_skipped"], stats["sentences_generated"],
                )

            grand_msgs    += ch_msgs
            grand_skipped += ch_skipped
            grand_sents   += ch_sents

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        print()
        print("=" * 65)
        print("  Summary")
        print("=" * 65)
        print(f"  Channels evaluated : {len(channels)}")
        print(f"  Channel-days run   : {total_days_run}")
        print(f"  Channel-days skip  : {total_days_skip}  (already completed)")
        print(f"  Msgs processed     : {grand_msgs}")
        print(f"  Msgs skipped       : {grand_skipped}  (empty after splitting)")
        print(f"  Sentences generated: {grand_sents}")
        print("=" * 65)

        log.info(
            "Run complete. days_run=%d days_skip=%d msgs=%d skipped=%d sentences=%d",
            total_days_run, total_days_skip, grand_msgs, grand_skipped, grand_sents,
        )

    finally:
        session.close()


# ===========================================================================
# CLI entry point
# ===========================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Myanmar sentence cleaner: splits TelegramMessage paragraphs "
            "and uploads CleanTeleText rows to Neon PostgreSQL.\n\n"
            "By default runs incrementally per channel: finds each channel's latest "
            "completed CleaningLog watermark and processes newer messages."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental (default): process messages since last completed watermark per channel
  uv run python services/telegram_scraper/cleaner.py

  # Process a specific channel only
  uv run python services/telegram_scraper/cleaner.py --channel shweba000

  # Dry-run preview of incremental pass
  uv run python services/telegram_scraper/cleaner.py --dry-run

  # Backfill the last 7 days (today + previous 6)
  uv run python services/telegram_scraper/cleaner.py --lookback 7

  # Backfill a specific date range
  uv run python services/telegram_scraper/cleaner.py --from-date 2025-08-01 --to-date 2025-08-07

  # Re-process (force) an already-completed date range
  uv run python services/telegram_scraper/cleaner.py --from-date 2025-08-01 --to-date 2025-08-07 --force

  # Use a custom bigram dictionary
  uv run python services/telegram_scraper/cleaner.py --dict ref/1syl.potma.dict
        """,
    )
    parser.add_argument("--config", default="config.yaml",
                        help="Config file name (default: config.yaml).")
    parser.add_argument("--channel", "-c", default=None, metavar="NAME",
                        help="Specific channel name to clean (default: all channels in config/DB).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview splits without writing to PostgreSQL.")
    parser.add_argument("--force", action="store_true",
                        help="Re-process already-completed day windows (deletes existing rows).")

    # Date range — two mutually exclusive ways to specify the window
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--lookback", type=int, default=None, metavar="DAYS",
                            help="Number of days to look back from today (inclusive). "
                                 "E.g. --lookback 7 = today + last 6 days. "
                                 "Mutually exclusive with --from-date.")
    date_group.add_argument("--from-date", metavar="YYYY-MM-DD", default=None,
                            help="Start of day range to clean (inclusive). "
                                 "Defaults to the day after the latest completed watermark. "
                                 "Mutually exclusive with --lookback.")

    parser.add_argument("--to-date", metavar="YYYY-MM-DD", default=None,
                        help="End of day range to clean (inclusive). "
                             "Defaults to today (UTC).")
    parser.add_argument("--dict", default=None, metavar="PATH",
                        help="Custom bigram dict file (one entry per line).")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG logging.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # --lookback N → from_date = today - (N-1) days  (same convention as scraper.py)
    resolved_from_date = args.from_date
    if args.lookback is not None:
        if args.lookback < 1:
            import sys as _sys
            print("error: --lookback must be at least 1", file=_sys.stderr)
            _sys.exit(1)
        lookback_start = datetime.now(timezone.utc).date() - timedelta(days=args.lookback - 1)
        resolved_from_date = lookback_start.strftime("%Y-%m-%d")

    cfg = load_config(args.config)
    clean_and_upload(
        config=cfg,
        channel=args.channel,
        dry_run=args.dry_run,
        force=args.force,
        dict_path=args.dict,
        from_date=resolved_from_date,
        to_date=args.to_date,
    )

