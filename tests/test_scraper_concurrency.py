import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from services.telegram_scraper.scraper import (
    resolve_channels_for_category,
    resolve_string_session,
    parse_channel_entity,
    build_day_windows
)
from shared.scraper_models import ScrapingErrorLog, init_scraper_db
from services.telegram_scraper.storage import StorageHandler


def test_channel_entity_parser():
    assert parse_channel_entity("@my_channel") == "my_channel"
    assert parse_channel_entity("my_channel") == "my_channel"
    assert parse_channel_entity("123456789") == 123456789
    assert parse_channel_entity("-100123456789") == -100123456789


def test_day_windows_generation():
    windows = build_day_windows(lookback_days=3)
    assert len(windows) == 3
    # Windows are chronological: oldest first
    assert windows[0][0] < windows[1][0] < windows[2][0]


def test_closed_day_windows_generation():
    now_utc = datetime.now(timezone.utc).date()
    yesterday_expected = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before_expected = (now_utc - timedelta(days=2)).strftime("%Y-%m-%d")

    # Yesterday only (closed_only=True, lookback_days=1)
    windows_1 = build_day_windows(lookback_days=1, closed_only=True)
    assert len(windows_1) == 1
    run_date, day_start, day_end = windows_1[0]
    assert run_date == yesterday_expected
    assert day_start.hour == 0 and day_start.minute == 0 and day_start.second == 0
    assert day_end.hour == 23 and day_end.minute == 59 and day_end.second == 59

    # 2 closed days
    windows_2 = build_day_windows(lookback_days=2, closed_only=True)
    assert len(windows_2) == 2
    assert windows_2[0][0] == day_before_expected
    assert windows_2[1][0] == yesterday_expected
    assert windows_2[0][0] < windows_2[1][0]


def test_resolve_channels_for_category():
    sample_config = {
        "scraping": {
            "categories": {
                "polarization": {
                    "channels": ["chan_polar_1", "chan_polar_2"]
                },
                "news": {
                    "channels": ["chan_news_1"]
                }
            }
        }
    }

    polar_ch, polar_cat = resolve_channels_for_category(sample_config, category="polarization")
    assert polar_cat == "polarization"
    assert polar_ch == ["chan_polar_1", "chan_polar_2"]

    news_ch, news_cat = resolve_channels_for_category(sample_config, category="news")
    assert news_cat == "news"
    assert news_ch == ["chan_news_1"]

    all_ch, all_cat = resolve_channels_for_category(sample_config, category=None)
    assert all_cat == "all"
    assert set(all_ch) == {"chan_polar_1", "chan_polar_2", "chan_news_1"}


def test_resolve_string_session_rotation():
    sample_config = {"telegram": {}}

    with patch.dict(os.environ, {
        "TELEGRAM_STRING_SESSION_1": "session_account_1",
        "TELEGRAM_STRING_SESSION_2": "session_account_2",
    }, clear=False):
        # Category polarization routes to session 1
        s_polar = resolve_string_session(sample_config, category="polarization")
        assert s_polar == "session_account_1"

        # Category news routes to session 2
        s_news = resolve_string_session(sample_config, category="news")
        assert s_news == "session_account_2"


def test_storage_dlq_logging(tmp_path):
    # Use SQLite for local storage testing
    db_file = tmp_path / "test_dlq.db"
    db_url = f"sqlite:///{db_file}"

    storage = StorageHandler(pg_url=db_url, schema="public")

    err_log = storage.log_scraping_error(
        category="polarization",
        channel_name="@failing_channel",
        run_date="2026-09-20",
        error_type="FloodWaitError",
        error_message="A wait of 420 seconds is required",
        stack_trace="Traceback: ...",
        retry_count=1
    )

    assert err_log.id is not None
    assert err_log.channel_name == "@failing_channel"
    assert err_log.error_type == "FloodWaitError"
    assert err_log.resolved is False

    retrieved = storage.session.query(ScrapingErrorLog).filter_by(channel_name="@failing_channel").first()
    assert retrieved is not None
    assert retrieved.category == "polarization"

    storage.close()
