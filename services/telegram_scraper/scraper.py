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
import traceback
import yaml
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add shared module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from services.telegram_scraper.storage import StorageHandler

load_dotenv()


# =============================================================================
# Config Loader & Environment Helper
# =============================================================================

def load_config(config_file=None):
    if not config_file:
        config_file = os.getenv("CONFIG_PATH", "config.yaml")
    if os.path.isabs(config_file):
        target_path = config_file
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        target_path = os.path.join(base_dir, config_file)
    if not os.path.exists(target_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        target_path = os.path.join(base_dir, "config.example.yaml")
    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_environment_and_schema(config: dict, env_override: str = None) -> tuple[str, str]:
    """
    Resolve environment ('dev' or 'prod') and corresponding PostgreSQL schema
    ('public' or 'production').
    Precedence: env_override (CLI) > ENVIRONMENT env var > config['environment'] > 'dev'.
    """
    env = env_override or os.getenv("ENVIRONMENT") or config.get("environment", "dev")
    env = str(env).strip().lower()
    if env in ("prod", "production"):
        return "prod", "production"
    return "dev", "public"


def resolve_channels_for_category(config: dict, category: str = None) -> tuple[list, str]:
    """
    Resolve channel list and active category name.
    Supports config.scraping.categories.<cat>.channels, config.scraping.channels.<cat>,
    or top-level channels list.
    """
    # Environment override takes precedence
    env_channels = os.getenv("TELEGRAM_CHANNELS")
    if env_channels:
        return [ch.strip() for ch in env_channels.split(",") if ch.strip()], (category or "custom")

    scraping_cfg = config.get("scraping", {})
    categories_cfg = scraping_cfg.get("categories", {})
    channels_cfg = scraping_cfg.get("channels", {})

    if category:
        cat_lower = category.strip().lower()
        # Check categories section first
        if isinstance(categories_cfg, dict) and cat_lower in categories_cfg:
            cat_data = categories_cfg[cat_lower]
            if isinstance(cat_data, dict):
                return cat_data.get("channels", []), cat_lower
            elif isinstance(cat_data, list):
                return cat_data, cat_lower
        # Check legacy channels mapping
        if isinstance(channels_cfg, dict) and cat_lower in channels_cfg:
            return channels_cfg[cat_lower], cat_lower
        return [], cat_lower

    # If no specific category requested, gather all distinct channels
    all_channels = []
    if isinstance(categories_cfg, dict):
        for cat_name, cat_val in categories_cfg.items():
            if isinstance(cat_val, dict):
                all_channels.extend(cat_val.get("channels", []))
            elif isinstance(cat_val, list):
                all_channels.extend(cat_val)
    if not all_channels and isinstance(channels_cfg, dict):
        for cat_name, ch_list in channels_cfg.items():
            if isinstance(ch_list, list):
                all_channels.extend(ch_list)
    elif not all_channels and isinstance(channels_cfg, list):
        all_channels = channels_cfg

    return list(dict.fromkeys(all_channels)), (category or "all")


def resolve_string_session(config: dict, category: str = None) -> str | None:
    """
    Resolves Telegram StringSession, supporting rotation via:
    - TELEGRAM_STRING_SESSION_1 / TELEGRAM_STRING_SESSION_2 (Airflow env vars)
    - TELEGRAM_STRING_SESSION
    - config['telegram']['string_sessions'] list
    - config['telegram']['string_session']
    """
    sessions = []
    s1 = os.getenv("TELEGRAM_STRING_SESSION_1")
    s2 = os.getenv("TELEGRAM_STRING_SESSION_2")
    if s1:
        sessions.append(s1)
    if s2:
        sessions.append(s2)

    s_default = os.getenv("TELEGRAM_STRING_SESSION")
    if s_default and s_default not in sessions:
        sessions.append(s_default)

    cfg_sessions = config.get("telegram", {}).get("string_sessions") or []
    for s in cfg_sessions:
        if s and s not in sessions:
            sessions.append(s)

    single_cfg = config.get("telegram", {}).get("string_session")
    if single_cfg and single_cfg not in sessions:
        sessions.append(single_cfg)

    if not sessions:
        return None

    if category and len(sessions) > 1:
        cat_lower = category.strip().lower()
        if "polar" in cat_lower:
            return sessions[0]
        elif "news" in cat_lower:
            return sessions[1]
        else:
            idx = abs(hash(category)) % len(sessions)
            return sessions[idx]

    return sessions[0]


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


def build_day_windows(lookback_days: int, closed_only: bool = False) -> list[tuple[str, datetime, datetime]]:
    """
    Returns a list of (run_date_str, day_start, day_end) tuples.
    If closed_only=False (default):
        covers today and the previous (lookback_days - 1) days.
        E.g. lookback_days=2 → [yesterday, today].
    If closed_only=True:
        covers only fully closed past days ending at yesterday (Day T-1).
        E.g. lookback_days=1, closed_only=True → [yesterday].
        E.g. lookback_days=2, closed_only=True → [day_before_yesterday, yesterday].
    Ordered chronologically (oldest first).
    """
    today_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_offset = 1 if closed_only else 0
    windows = []
    for offset in range(lookback_days + end_offset - 1, end_offset - 1, -1):   # oldest → newest
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

async def scrape_channel_for_window(client, channel, day_start: datetime, day_end: datetime, limit: int, category: str = None) -> list[dict]:
    """
    Fetch messages from a single channel between day_start and day_end (UTC).
    Uses offset_date (= day_end + 1s) so Telegram returns messages <= day_end,
    then filters out messages before day_start — matching the reference approach.
    """
    from telethon.tl.functions.messages import GetHistoryRequest

    target_entity = parse_channel_entity(channel)
    messages_in_window = []

    async def _iter_channel(entity):
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
                if msg_date < day_start:
                    return
                messages_in_window.append({
                    "channel_name": str(channel),
                    "category":     category,
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
        if isinstance(target_entity, int) and target_entity > 0:
            alt_entity = int(f"-100{target_entity}")
            print(f"  [*] Retrying with prefixed ID: {alt_entity}...")
            try:
                await _iter_channel(alt_entity)
            except Exception as alt_err:
                print(f"  [!] Retry failed for {alt_entity}: {alt_err}")
                raise alt_err
        else:
            raise primary_err

    return messages_in_window


async def run_scraper(dry_run: bool = False, lookback_days: int = 2, force: bool = False,
                      config_file: str = None, env: str = None, category: str = None,
                      yesterday: bool = False, closed_only: bool = False):
    config = load_config(config_file)
    environment, schema = resolve_environment_and_schema(config, env_override=env)

    api_id   = os.getenv("TELEGRAM_API_ID")  or config.get("telegram", {}).get("api_id")
    api_hash = os.getenv("TELEGRAM_API_HASH") or config.get("telegram", {}).get("api_hash")

    channels, active_category = resolve_channels_for_category(config, category=category)

    env_limit = os.getenv("TELEGRAM_LIMIT_PER_CHANNEL")
    limit = int(env_limit) if env_limit else config.get("scraping", {}).get("limit_per_channel", 200)

    max_parallel = int(config.get("scraping", {}).get("max_parallel_channels", 5))
    inter_channel_delay = int(config.get("scraping", {}).get("inter_channel_delay_seconds", 60))
    chunk_size = int(config.get("scraping", {}).get("db_upload_chunk_size", 1000))

    pg_url = (
        os.getenv("NEON_DATABASE_URL")
        or config.get("postgresql", {}).get("url", "")
    )

    if yesterday:
        lookback_days = 1
        closed_only = True

    day_windows = build_day_windows(lookback_days, closed_only=closed_only)

    if yesterday:
        target_desc = f"Yesterday (Closed Day T-1: {day_windows[0][0]})"
    elif closed_only:
        target_desc = f"{lookback_days} closed day(s) ({day_windows[0][0]} → {day_windows[-1][0]})"
    else:
        target_desc = f"{lookback_days} day(s) ({day_windows[0][0]} → {day_windows[-1][0]})"

    print("=" * 65)
    print(f"  🤖 Telegram Scraper — Timestamp & Concurrency Mode")
    print("=" * 65)
    print(f"  Environment   : {environment} (schema: {schema})")
    print(f"  Category      : {active_category}")
    print(f"  Channels ({len(channels)}) : {channels}")
    print(f"  Max Parallel  : {max_parallel}")
    print(f"  Delay Between : {inter_channel_delay}s")
    print(f"  Target Window : {target_desc}")
    print(f"  Dry-run       : {dry_run}")
    print(f"  Force         : {force}")
    print("=" * 65)

    storage = StorageHandler(pg_url=pg_url, schema=schema)

    is_placeholder = (
        not api_id or not api_hash
        or "YOUR_TELEGRAM_API_ID"   in str(api_id)
        or "YOUR_TELEGRAM_API_HASH" in str(api_hash)
    )

    if is_placeholder:
        print("[!] API credentials are placeholders — running in simulated mode.\n")
        _run_simulated(storage, channels, active_category, day_windows, dry_run, force)
        storage.close()
        return

    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        string_session_val = resolve_string_session(config, category=active_category)
        if string_session_val:
            print(f"[*] Connecting to Telegram using StringSession for category '{active_category}'...")
            client = TelegramClient(StringSession(string_session_val), int(api_id), api_hash,
                                    connection_retries=1, timeout=10)
        else:
            session_name = config.get("telegram", {}).get("session_name", "telegram_scraper")
            session_dir = os.getenv("TELEGRAM_SESSION_DIR", os.path.dirname(__file__))
            if not os.path.isabs(session_name):
                session_path = os.path.join(session_dir, session_name)
            else:
                session_path = session_name
            print(f"[*] Connecting to Telegram (session: {session_path})...")
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
                client=client,
                storage=storage,
                channels=channels,
                category=active_category,
                run_date=run_date,
                day_start=day_start,
                day_end=day_end,
                limit=limit,
                dry_run=dry_run,
                force=force,
                max_parallel=max_parallel,
                inter_channel_delay=inter_channel_delay,
                chunk_size=chunk_size
            )

        await client.disconnect()

    except Exception as e:
        print(f"[!] Telegram connection error: {e}\n    Falling back to simulated mode.")
        _run_simulated(storage, channels, active_category, day_windows, dry_run, force)

    storage.close()


async def _process_day(client, storage, channels: list, category: str,
                       run_date: str, day_start: datetime, day_end: datetime,
                       limit: int, dry_run: bool, force: bool,
                       max_parallel: int = 5, inter_channel_delay: int = 60,
                       chunk_size: int = 1000):
    """
    Scrape one calendar day across channels with controlled concurrency and throttling.
    Failed channels are recorded to scraping_error_logs (DLQ) without failing the whole batch.
    """
    print(f"\n📅  Processing day: {run_date}  [{day_start.isoformat()} → {day_end.isoformat()}]")

    semaphore = asyncio.Semaphore(max_parallel)
    results = {"saved": 0, "skipped": 0, "scraped": 0, "errors": 0}

    async def _scrape_single_channel(channel):
        async with semaphore:
            loop = asyncio.get_running_loop()
            start_time = loop.time()
            print(f"  [*] [Worker] Scraping channel: {channel} ...")

            if not dry_run and not force and storage.channel_day_already_scraped(str(channel), run_date):
                print(f"      ✅ Watermark found — channel {channel} for {run_date} already completed, skipping.")
                return

            log = None
            if not dry_run:
                if force:
                    deleted = storage.delete_messages_for_channel_window(str(channel), day_start, day_end)
                    if deleted:
                        print(f"      --force: deleted {deleted} existing messages for {channel} in window.")
                log = storage.start_scraping_log(str(channel), run_date, day_start, day_end, category=category, force=force)

            try:
                msgs = await scrape_channel_for_window(client, channel, day_start, day_end, limit, category=category)
                print(f"      [{channel}] Found {len(msgs)} messages in window.")
                results["scraped"] += len(msgs)

                ch_saved, ch_skipped = 0, 0
                if not dry_run and msgs:
                    # Upload in chunks (relaxing load every chunk_size)
                    for i in range(0, len(msgs), chunk_size):
                        chunk = msgs[i:i + chunk_size]
                        s_count, sk_count = storage.save_messages(chunk)
                        ch_saved += s_count
                        ch_skipped += sk_count
                        if i + chunk_size < len(msgs):
                            await asyncio.sleep(1)

                    print(f"      [{channel}] Saved={ch_saved} Skipped={ch_skipped}")
                    results["saved"] += ch_saved
                    results["skipped"] += ch_skipped
                    storage.finish_scraping_log(log, len(msgs), ch_saved, ch_skipped, status='completed')
                elif not dry_run and not msgs:
                    storage.finish_scraping_log(log, 0, 0, 0, status='completed')

            except Exception as ch_err:
                results["errors"] += 1
                err_type = type(ch_err).__name__
                print(f"      ❌ [{channel}] Scraping failed ({err_type}): {ch_err}")
                if not dry_run:
                    storage.log_scraping_error(
                        category=category,
                        channel_name=str(channel),
                        run_date=run_date,
                        error_type=err_type,
                        error_message=str(ch_err),
                        stack_trace=traceback.format_exc(),
                        retry_count=0
                    )
                    if log:
                        storage.finish_scraping_log(log, 0, 0, 0, status='failed')

            finally:
                elapsed = loop.time() - start_time
                if elapsed < inter_channel_delay:
                    sleep_time = inter_channel_delay - elapsed
                    await asyncio.sleep(sleep_time)

    # Concurrently execute all channels under semaphore throttle
    await asyncio.gather(*[_scrape_single_channel(ch) for ch in channels])

    if dry_run:
        print(f"  [DRY-RUN] {run_date}: Scraped {results['scraped']} messages across {len(channels)} channels.")
    else:
        print(f"  ✔  Day {run_date} complete — Saved={results['saved']}, Skipped={results['skipped']}, Errors={results['errors']}")


def _run_simulated(storage, channels: list, category: str, day_windows: list, dry_run: bool, force: bool):
    """Stub fallback mode — generates sample messages per day window."""
    for run_date, day_start, day_end in day_windows:
        print(f"\n📅  [Simulated] Processing day: {run_date} (Category: {category})")

        simulated_saved = 0
        simulated_skipped = 0

        for ch in channels:
            if not dry_run and not force and storage.channel_day_already_scraped(str(ch), run_date):
                print(f"  ✅ Watermark found — channel {ch} for {run_date} already completed, skipping.")
                continue

            if not dry_run:
                if force:
                    storage.delete_messages_for_channel_window(str(ch), day_start, day_end)
                log = storage.start_scraping_log(str(ch), run_date, day_start, day_end, category=category, force=force)
            else:
                log = None

            simulated = []
            for i in range(1, 4):
                simulated.append({
                    "channel_name": str(ch),
                    "category":     category,
                    "message_id":   int(day_start.strftime("%Y%m%d")) * 100 + i,
                    "message_text": f"[{run_date}] [{category}] Sample message #{i} from channel {ch}.",
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
        description="Concurrent Telegram Scraper with multi-session rotation and DLQ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Daily closed-day production batch (yesterday only, zero gaps)
  python services/telegram_scraper/scraper.py --category news --yesterday

  # Scrape specific category (e.g. polarization or news)
  python services/telegram_scraper/scraper.py --category polarization

  # Dry-run preview
  python services/telegram_scraper/scraper.py --category news --dry-run

  # Backfill last 7 days
  python services/telegram_scraper/scraper.py --category polarization --lookback 7
        """
    )
    parser.add_argument(
        "--config", default=None, metavar="PATH",
        help="Path to custom config YAML file (or set CONFIG_PATH env var)"
    )
    parser.add_argument(
        "--env", choices=["dev", "prod"], default=None,
        help="Environment to target ('dev' -> public schema, 'prod' -> production schema). Overrides config.yaml."
    )
    parser.add_argument(
        "--category", default=None,
        help="Category to target (e.g. 'polarization' or 'news'). Overrides all channels."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview scraping without writing to DB"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-scrape already-completed channel-day windows (overwrites existing messages)"
    )

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--yesterday", action="store_true",
        help="Scrape only yesterday's closed 24-hour calendar day (00:00:00 to 23:59:59 UTC). "
             "Recommended for daily Airflow runs to avoid watermark gaps and duplicate API calls."
    )
    date_group.add_argument(
        "--lookback", type=int, default=2, metavar="DAYS",
        help="Number of days to look back (default: 2 = today + yesterday). Mutually exclusive with --yesterday."
    )

    args = parser.parse_args()

    if not args.yesterday and args.lookback < 1:
        parser.error("--lookback must be at least 1")

    asyncio.run(run_scraper(
        dry_run=args.dry_run,
        lookback_days=1 if args.yesterday else args.lookback,
        force=args.force,
        config_file=args.config,
        env=args.env,
        category=args.category,
        yesterday=args.yesterday,
        closed_only=args.yesterday,
    ))


