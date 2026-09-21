import os
import sys
import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from shared.scraper_models import (
    ScraperBase,
    TelegramMessage,
    ScrapingLog,
    apply_scraper_migrations,
    _extract_channel_category_mapping,
)
from services.telegram_scraper.storage import StorageHandler


@pytest.fixture
def test_config():
    return {
        "scraping": {
            "categories": {
                "polarization": {
                    "channels": ["shweba000", "@kyawswar49111", "@SittKhwayDead"]
                },
                "news": {
                    "channels": ["@khitthitnews", "@theirrawaddy"]
                }
            }
        }
    }


def test_extract_channel_category_mapping(test_config):
    mapping = _extract_channel_category_mapping(test_config)
    # Check normalized forms with and without @
    assert mapping["shweba000"] == "polarization"
    assert mapping["@shweba000"] == "polarization"
    assert mapping["@kyawswar49111"] == "polarization"
    assert mapping["kyawswar49111"] == "polarization"
    assert mapping["@khitthitnews"] == "news"
    assert mapping["khitthitnews"] == "news"


def test_migrate_script_channel_normalization_and_matching(test_config):
    from services.telegram_scraper.migrate_add_category import (
        normalize_channel_name,
        extract_category_mappings,
        match_category,
    )

    # Test normalization
    assert normalize_channel_name("@shweba000") == "shweba000"
    assert normalize_channel_name("shweba000") == "shweba000"
    assert normalize_channel_name("@SittKhwayDead") == "sittkhwaydead"
    assert normalize_channel_name("https://t.me/theirrawaddy") == "theirrawaddy"
    assert normalize_channel_name("t.me/@khitthitnews") == "khitthitnews"

    # Test canonical mappings
    cat_map = extract_category_mappings(test_config)
    assert cat_map["shweba000"] == "polarization"
    assert cat_map["kyawswar49111"] == "polarization"
    assert cat_map["sittkhwaydead"] == "polarization"
    assert cat_map["khitthitnews"] == "news"
    assert cat_map["theirrawaddy"] == "news"

    # Test matching with and without @
    assert match_category("shweba000", cat_map) == "polarization"
    assert match_category("@shweba000", cat_map) == "polarization"
    assert match_category("@SittKhwayDead", cat_map) == "polarization"
    assert match_category("sittkhwaydead", cat_map) == "polarization"
    assert match_category("@khitthitnews", cat_map) == "news"
    assert match_category("khitthitnews", cat_map) == "news"
    assert match_category("@unknown_channel", cat_map) is None


def test_migration_adds_column_and_preserves_data_with_backfill(test_config, tmp_path):
    db_file = tmp_path / "test_legacy.db"
    engine = create_engine(f"sqlite:///{db_file}")

    # 1. Create tables mimicking legacy schema (NO 'category' column)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE telegram_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name VARCHAR(100) NOT NULL,
                message_id BIGINT NOT NULL,
                message_text TEXT NOT NULL,
                date TIMESTAMP,
                media_url VARCHAR(500),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP
            );
        """))
        conn.execute(text("""
            CREATE TABLE scraping_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name VARCHAR(100) NOT NULL,
                run_date VARCHAR(10) NOT NULL,
                messages_scraped INTEGER DEFAULT 0,
                messages_saved INTEGER DEFAULT 0,
                messages_skipped INTEGER DEFAULT 0,
                scrape_start_ts TIMESTAMP NOT NULL,
                scrape_end_ts TIMESTAMP NOT NULL,
                run_started_at TIMESTAMP,
                run_finished_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'running'
            );
        """))

        # 2. Insert existing data rows
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(text("""
            INSERT INTO telegram_messages (channel_name, message_id, message_text, date, created_at)
            VALUES
                ('@shweba000', 101, 'Polarization post 1', :now, :now),
                ('khitthitnews', 102, 'News post 1', :now, :now),
                ('unknown_channel', 103, 'Old historical unmapped post', :now, :now);
        """), {"now": now})

        conn.execute(text("""
            INSERT INTO scraping_logs (channel_name, run_date, scrape_start_ts, scrape_end_ts)
            VALUES
                ('@shweba000', '2026-08-01', :now, :now),
                ('khitthitnews', '2026-08-01', :now, :now),
                ('unknown_channel', '2026-08-01', :now, :now);
        """), {"now": now})
        conn.commit()

    # Verify column does not exist initially
    inspector_before = inspect(engine)
    cols_msg_before = [c["name"] for c in inspector_before.get_columns("telegram_messages")]
    assert "category" not in cols_msg_before

    # 3. Apply migration & backfill
    results = apply_scraper_migrations(engine, schema="public", config=test_config)

    assert "telegram_messages" in results["columns_added"]
    assert "scraping_logs" in results["columns_added"]
    assert results["backfilled_messages"] >= 2
    assert results["backfilled_logs"] >= 2

    # 4. Verify existing data was preserved and backfilled
    inspector_after = inspect(engine)
    cols_msg_after = [c["name"] for c in inspector_after.get_columns("telegram_messages")]
    assert "category" in cols_msg_after

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT channel_name, message_id, message_text, category FROM telegram_messages ORDER BY id ASC")).fetchall()
        assert len(rows) == 3

        # @shweba000 was backfilled to polarization
        assert rows[0][0] == "@shweba000"
        assert rows[0][1] == 101
        assert rows[0][2] == "Polarization post 1"
        assert rows[0][3] == "polarization"

        # khitthitnews was backfilled to news
        assert rows[1][0] == "khitthitnews"
        assert rows[1][1] == 102
        assert rows[1][2] == "News post 1"
        assert rows[1][3] == "news"

        # unknown_channel was filled with blank value ''
        assert rows[2][0] == "unknown_channel"
        assert rows[2][1] == 103
        assert rows[2][2] == "Old historical unmapped post"
        assert rows[2][3] == ""

        # Check scraping_logs backfill
        log_rows = conn.execute(text("SELECT channel_name, category FROM scraping_logs ORDER BY id ASC")).fetchall()
        assert len(log_rows) == 3
        assert log_rows[0][1] == "polarization"
        assert log_rows[1][1] == "news"
        assert log_rows[2][1] == ""

    # 5. Verify new ORM operations work with the migrated tables
    Session = sessionmaker(bind=engine)
    session = Session()

    new_msg = TelegramMessage(
        channel_name="@theirrawaddy",
        category="news",
        message_id=201,
        message_text="Brand new post",
        date=datetime.now(timezone.utc),
        status="pending"
    )
    session.add(new_msg)
    session.commit()

    saved_row = session.query(TelegramMessage).filter_by(message_id=201).first()
    assert saved_row is not None
    assert saved_row.category == "news"
    session.close()


def test_storage_handler_save_messages_and_start_log_with_category(tmp_path):
    db_file = tmp_path / "test_storage.db"
    pg_url = f"sqlite:///{db_file}"
    storage = StorageHandler(pg_url=pg_url)

    # 1. Start scraping log with category
    now = datetime.now(timezone.utc)
    log = storage.start_scraping_log(
        channel_name="shweba000",
        run_date="2026-08-02",
        scrape_start_ts=now,
        scrape_end_ts=now,
        category="polarization",
    )
    assert log.category == "polarization"

    # 2. Save messages with category
    messages = [
        {
            "channel_name": "shweba000",
            "category": "polarization",
            "message_id": 999,
            "message_text": "Testing category save",
            "date": now,
        }
    ]
    saved, skipped = storage.save_messages(messages)
    assert saved == 1
    assert skipped == 0

    msg = storage.session.query(TelegramMessage).filter_by(message_id=999).first()
    assert msg is not None
    assert msg.category == "polarization"
    storage.close()
