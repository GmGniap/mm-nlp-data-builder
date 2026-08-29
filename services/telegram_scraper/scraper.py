#!/usr/bin/env python3
"""
Telegram Scraping Script
==============================================
To scrape messages from provided channels in config.yml.

Usage:
  # Default: scrape today + yesterday
  uv run python services/telegram_scraper/scraper.py

  # Dry-run: preview without writing to DB
  uv run python services/telegram_scraper/scraper.py --dry-run

  # Backfill last 7 days
  uv run python services/telegram_scraper/scraper.py --lookback 7

  # Backfill last 30 days (dry-run preview)
  uv run python services/telegram_scraper/scraper.py --lookback 30 --dry-run 
"""
import os
import sys
import argparse
import asyncio
import yaml
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add shared module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from services.telegram_scraper.storage import StorageHandler

load_dotenv()


# =============================================================================
# Config Loader
# =============================================================================

def load_config(config_file="config.yaml"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(base_dir, config_file)
    if not os.path.exists(target_path):
        target_path = os.path.join(base_dir, "config.example.yaml")
    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# =============================================================================
# Timestamp Window Helpers  (reference: Amirwpi/Telegram_Scraper offset_date)
# =============================================================================

def get_day_window(date_utc: datetime) -> tuple[datetime, datetime]:
    """
    Returns (start_of_day, end_of_day) as timezone-aware UTC datetimes
    for the given UTC date.
    """
    day_start = date_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    day_end   = day_start + timedelta(days=1) - timedelta(seconds=1)
    return day_start, day_end


def build_day_windows(lookback_days: int) -> list[tuple[str, datetime, datetime]]:
    """
    Returns a list of (run_date_str, day_start, day_end) tuples
    covering today and the previous (lookback_days - 1) days.
    E.g. lookback_days=2 → [yesterday, today].
    Ordered chronologically (oldest first).
    """
    today_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    windows = []
    for offset in range(lookback_days - 1, -1, -1):   # oldest → newest
        day = today_utc - timedelta(days=offset)
        day_start, day_end = get_day_window(day)
        windows.append((day.strftime("%Y-%m-%d"), day_start, day_end))
    return windows


# =============================================================================
# Channel Entity Parser
# =============================================================================

def parse_channel_entity(channel):
    """
    Supports handles (@channel), plain usernames, positive numeric IDs
    (1667647977) and negative channel IDs (-1001667647977).
    """
    ch_str = str(channel).strip()
    if ch_str.lstrip('-').isdigit():
        return int(ch_str)
    if ch_str.startswith('@'):
        return ch_str[1:]
    return ch_str


# =============================================================================
# Core Scraper
# =============================================================================

async def scrape_channel_for_window(client, channel, day_start: datetime, day_end: datetime, limit: int) -> list[dict]:
    """
    Fetch messages from a single channel between day_start and day_end (UTC).
    Uses offset_date (= day_end + 1s) so Telegram returns messages <= day_end,
    then filters out messages before day_start — matching the reference approach.
    """
    from telethon.tl.functions.messages import GetHistoryRequest

    target_entity = parse_channel_entity(channel)
    messages_in_window = []

    async def _iter_channel(entity):
        # offset_date = day_end + 1 second so Telegram returns messages on/before day_end
        offset_date = day_end + timedelta(seconds=1)

        peer = await client.get_entity(entity)
        offset_id   = 0
        remaining   = limit

        while remaining > 0:
            batch_size = min(100, remaining)
            history = await client(GetHistoryRequest(
                peer=peer,
                limit=batch_size,
                offset_date=offset_date,
                offset_id=offset_id,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            if not history.messages:
                break

            for msg in history.messages:
                if not msg.message:
                    continue
                msg_date = msg.date.replace(tzinfo=timezone.utc) if msg.date.tzinfo is None else msg.date
                # Stop if we've gone past the start of our window
                if msg_date < day_start:
                    return
                messages_in_window.append({
                    "channel_name": str(channel),
                    "message_id":   msg.id,
                    "message_text": msg.message,
                    "date":         msg_date,
                    "media_url":    None
                })

            last_msg  = history.messages[-1]
            offset_id = last_msg.id
            last_date = last_msg.date.replace(tzinfo=timezone.utc) if last_msg.date.tzinfo is None else last_msg.date
            if last_date < day_start:
                break

            remaining -= len(history.messages)

    try:
        await _iter_channel(target_entity)
    except Exception as primary_err:
        print(f"  [!] Primary fetch error for {target_entity}: {primary_err}")
        # Retry with -100 prefix for raw positive numeric IDs
        if isinstance(target_entity, int) and target_entity > 0:
            alt_entity = int(f"-100{target_entity}")
            print(f"  [*] Retrying with prefixed ID: {alt_entity}...")
            try:
                await _iter_channel(alt_entity)
            except Exception as alt_err:
                print(f"  [!] Retry failed for {alt_entity}: {alt_err}")

    return messages_in_window


async def run_scraper(dry_run: bool = False, lookback_days: int = 2):
    config  = load_config()

    api_id   = os.getenv("TELEGRAM_API_ID")  or config["telegram"].get("api_id")
    api_hash = os.getenv("TELEGRAM_API_HASH") or config["telegram"].get("api_hash")
    channels = config["scraping"].get("channels", [])
    limit    = config["scraping"].get("limit_per_channel", 200)
    pg_url   = (
        os.getenv("NEON_DATABASE_URL")
        or config.get("postgresql", {}).get("url", "")
    )

    day_windows = build_day_windows(lookback_days)

    print("=" * 65)
    print(f"  🤖 Telegram Scraper — Timestamp-based mode")
    print("=" * 65)
    print(f"  Channels     : {channels}")
    print(f"  Lookback days: {lookback_days}  ({day_windows[0][0]} → {day_windows[-1][0]})")
    print(f"  Dry-run      : {dry_run}")
    print("=" * 65)

    storage = StorageHandler(pg_url=pg_url)

    is_placeholder = (
        not api_id or not api_hash
        or "YOUR_TELEGRAM_API_ID"   in str(api_id)
        or "YOUR_TELEGRAM_API_HASH" in str(api_hash)
    )

    if is_placeholder:
        print("[!] API credentials are placeholders — running in simulated mode.\n")
        _run_simulated(storage, channels, day_windows, dry_run)
        storage.close()
        return

    try:
        from telethon import TelegramClient

        session_name = config["telegram"].get("session_name", "telegram_scraper")
        session_path = os.path.join(os.path.dirname(__file__), session_name)

        print(f"[*] Connecting to Telegram (session: {session_name})...")
        client = TelegramClient(session_path, int(api_id), api_hash,
                                connection_retries=1, timeout=10)
        await client.connect()

        if not await client.is_user_authorized():
            print("\n" + "!" * 65)
            print(" ⚠️  TELETHON CLIENT NOT AUTHORIZED")
            print("!" * 65)
            print("  Run: uv run python services/telegram_scraper/login_telegram.py")
            print("!" * 65 + "\n")
            await client.disconnect()
            storage.close()
            return

        for run_date, day_start, day_end in day_windows:
            await _process_day(
                client, storage, channels, run_date,
                day_start, day_end, limit, dry_run
            )

        await client.disconnect()

    except Exception as e:
        print(f"[!] Telegram connection error: {e}\n    Falling back to simulated mode.")
        _run_simulated(storage, channels, day_windows, dry_run)

    storage.close()


async def _process_day(client, storage, channels: list,
                       run_date: str, day_start: datetime, day_end: datetime,
                       limit: int, dry_run: bool):
    """
    Scrape one calendar day across all configured channels.
    Idempotent: skips the day if a 'completed' watermark already exists.
    """
    print(f"\n📅  Processing day: {run_date}  [{day_start.isoformat()} → {day_end.isoformat()}]")

    # --- Watermark check --- (Moved inside channel loop)
    total_scraped = 0
    total_saved   = 0
    total_skipped = 0

    for channel in channels:
        print(f"  [*] Scraping channel {channel} ...")
        
        if not dry_run and storage.channel_day_already_scraped(str(channel), run_date):
            print(f"      ✅ Watermark found — channel {channel} for day {run_date} already completed, skipping.")
            continue

        log = None if dry_run else storage.start_scraping_log(str(channel), run_date, day_start, day_end)

        msgs = await scrape_channel_for_window(client, channel, day_start, day_end, limit)
        print(f"      Found {len(msgs)} messages in window.")
        total_scraped += len(msgs)

        if not dry_run and msgs:
            saved, skipped = storage.save_messages(msgs)
            print(f"      Saved={saved}  Skipped={skipped}")
            total_saved   += saved
            total_skipped += skipped
            storage.finish_scraping_log(log, len(msgs), saved, skipped)
        elif not dry_run and not msgs:
            storage.finish_scraping_log(log, 0, 0, 0)

    if dry_run:
        print(f"  [DRY-RUN] {run_date}: scraped {total_scraped} messages across {len(channels)} channel(s).")
    else:
        print(f"  ✔  Day {run_date} complete — Saved={total_saved}, Skipped={total_skipped}")


def _run_simulated(storage, channels: list, day_windows: list, dry_run: bool):
    """Stub fallback mode — generates sample messages per day window."""
    for run_date, day_start, day_end in day_windows:
        print(f"\n📅  [Simulated] Processing day: {run_date}")

        simulated_saved = 0
        simulated_skipped = 0

        for ch in channels:
            if not dry_run and storage.channel_day_already_scraped(str(ch), run_date):
                print(f"  ✅ Watermark found — channel {ch} for {run_date} already completed, skipping.")
                continue

            log = None if dry_run else storage.start_scraping_log(str(ch), run_date, day_start, day_end)
            simulated = []
            for i in range(1, 4):
                simulated.append({
                    "channel_name": str(ch),
                    "message_id":   int(day_start.strftime("%Y%m%d")) * 100 + i,
                    "message_text": f"[{run_date}] Sample message #{i} from channel {ch}.",
                    "date":         day_start + timedelta(hours=i),
                    "media_url":    None
                })

            if not dry_run:
                saved, skipped = storage.save_messages(simulated)
                simulated_saved += saved
                simulated_skipped += skipped
                storage.finish_scraping_log(log, len(simulated), saved, skipped)
            elif dry_run:
                simulated_saved += len(simulated)

        if dry_run:
            print(f"  [DRY-RUN] Would save {simulated_saved} simulated messages across channels.")
        else:
            print(f"  Simulated — Saved={simulated_saved}, Skipped={simulated_skipped}")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Timestamp-based Telegram Channel Scraper with day watermark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: scrape today + yesterday
  uv run python services/telegram_scraper/scraper.py

  # Dry-run: preview without writing to DB
  uv run python services/telegram_scraper/scraper.py --dry-run

  # Backfill last 7 days
  uv run python services/telegram_scraper/scraper.py --lookback 7

  # Backfill last 30 days (dry-run preview)
  uv run python services/telegram_scraper/scraper.py --lookback 30 --dry-run
        """
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview scraping without writing to DB"
    )
    parser.add_argument(
        "--lookback", type=int, default=2, metavar="DAYS",
        help="Number of days to look back (default: 2 = today + yesterday)"
    )
    args = parser.parse_args()

    if args.lookback < 1:
        parser.error("--lookback must be at least 1")

    asyncio.run(run_scraper(dry_run=args.dry_run, lookback_days=args.lookback))
