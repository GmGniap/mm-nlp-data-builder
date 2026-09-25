import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.annotation_models import (
    AnnotationBase,
    CleanTeleText,
    CleanTeleExtraInfo,
    CleaningLog,
    CleaningErrorLog,
    apply_annotation_migrations,
)
from shared.scraper_models import ScraperBase, TelegramMessage
from services.telegram_scraper.cleaner import (
    normalize_myanmar_digits,
    parse_news_message,
    clean_polarization_sentence,
    extract_telegram_channels,
    split_myanmar_sentences,
    stage_channel_records,
    upload_staged_data,
    retry_dlq,
    _process_channel_day,
)


@pytest.fixture
def mock_config():
    return {
        "scraping": {
            "categories": {
                "polarization": {
                    "channels": ["shweba000", "kyawswar49111"]
                },
                "news": {
                    "channels": ["khitthitnews", "theirrawaddy"]
                },
            }
        }
    }


def test_normalize_myanmar_digits():
    assert normalize_myanmar_digits("၁၂၃၄၅၆၇၈၉၀") == "1234567890"
    assert normalize_myanmar_digits("မကွေး ၂၀ ရက်") == "မကွေး 20 ရက်"


def test_parse_news_message():
    raw_news = (
        "မကွေးတိုင်းတွင် တိုက်ပွဲပြင်းထန်\n"
        "မကွေး၊ စက်တင်ဘာ ၂၀ ရက်\n"
        "စစ်တပ်က အင်အားသုံးတိုက်ခိုက်ခဲ့သည်။ ပြည်သူ့ကာကွယ်ရေးတပ်က ပြန်လည်ခုခံခဲ့သည်။\n"
        "အသေးစိတ်ကို https://example.com/news/123 နှင့် https://youtube.com/watch?v=abc တွင် ဖတ်ရှုနိုင်ပါသည်။"
    )
    # 1. With run_date provided (formats to ISO YYYY-MM-DD)
    headline, clean_info_date, original_short_note, urls, body = parse_news_message(raw_news, run_date="2026-09-20")

    assert headline == "မကွေးတိုင်းတွင် တိုက်ပွဲပြင်းထန်"
    assert clean_info_date == "2026-09-20"
    assert "မကွေး" not in clean_info_date
    assert original_short_note == "မကွေး၊ စက်တင်ဘာ ၂၀ ရက်"
    assert "https://example.com/news/123" in urls
    assert "https://youtube.com/watch?v=abc" in urls
    assert "စစ်တပ်က အင်အားသုံးတိုက်ခိုက်ခဲ့သည်။" in body

    # 2. Without run_date (defaults year to current scraping time year -> YYYY-MM-DD)
    current_year = datetime.now(timezone.utc).year
    _, clean_info_date_no_yr, orig_note, _, _ = parse_news_message(raw_news)
    assert clean_info_date_no_yr == f"{current_year}-09-20"
    assert orig_note == "မကွေး၊ စက်တင်ဘာ ၂၀ ရက်"

    # 3. With explicit year in Myanmar text (formats to YYYY-MM-DD)
    raw_with_yr = (
        "ခေါင်းစဉ်အသစ်\n"
        "ရန်ကုန်၊ ၂၀၂၃ ခုနှစ်၊ မတ် ၁၅ ရက်\n"
        "သတင်းအကြောင်းအရာ။"
    )
    _, clean_date_yr, orig_note_yr, _, _ = parse_news_message(raw_with_yr)
    assert clean_date_yr == "2023-03-15"
    assert orig_note_yr == "ရန်ကုန်၊ ၂၀၂၃ ခုနှစ်၊ မတ် ၁၅ ရက်"

    # 4. Numerical dateline (e.g. DD-MM-YYYY)
    raw_num_date = (
        "သတင်းခေါင်းစဉ်\n"
        "မန္တလေး၊ ၁၅-၃-၂၀၂၄\n"
        "သတင်းအချက်အလက်။"
    )
    _, clean_date_num, orig_num, _, _ = parse_news_message(raw_num_date)
    assert clean_date_num == "2024-03-15"
    assert orig_num == "မန္တလေး၊ ၁၅-၃-၂၀၂၄"


def test_clean_polarization_sentence_filters():
    # 1. Purely English sentence -> dropped
    assert clean_polarization_sentence("Breaking news: This is only English text!") is None

    # 2. Too short (< 8 syllables/words) -> dropped
    assert clean_polarization_sentence("မင်္ဂလာပါ ခင်ဗျာ။") is None

    # 3. Emojis and noise stripped, but valid sentence kept if >= 8 syllables
    noisy_sentence = "🔥🚨 စစ်တပ်က နေပြည်တော်တွင် ပြည်သူများကို ဖမ်းဆီးနေကြောင်း သိရှိရပါသည်။ 🚨💥"
    cleaned = clean_polarization_sentence(noisy_sentence)
    assert cleaned is not None
    assert "🔥" not in cleaned
    assert "🚨" not in cleaned
    assert "စစ်တပ်က နေပြည်တော်တွင်" in cleaned


def test_telegram_channel_discovery():
    text_sample = "သတင်းအပြည့်အစုံကို t.me/unmonitored_news_channel နှင့် @secret_channel တွင် ကြည့်ရှုပါ။"
    channels = extract_telegram_channels(text_sample)
    assert "unmonitored_news_channel" in channels
    assert "secret_channel" in channels


def test_stage_channel_records_and_bulk_upload(tmp_path):
    stage_dir = str(tmp_path / "staging")
    db_file = str(tmp_path / "test_stage.db")
    pg_url = f"sqlite:///{db_file}"
    engine = create_engine(pg_url)
    AnnotationBase.metadata.create_all(engine)

    # 1. Stage records to JSONL
    text_rows = [
        {
            "telegram_message_id": 1,
            "line_index": 0,
            "sentence": "သတင်းခေါင်းစဉ်ဖြစ်ပါသည်။",
            "channel_name": "khitthitnews",
            "category": "news",
            "source_message_id": 100,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "telegram_message_id": 1,
            "line_index": 1,
            "sentence": "ဒုတိယစာကြောင်းဖြစ်ပါသည်။",
            "channel_name": "khitthitnews",
            "category": "news",
            "source_message_id": 100,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    ]
    extra_rows = [
        {
            "channel_name": "khitthitnews",
            "category": "news",
            "message_id": 100,
            "headline": "သတင်းခေါင်းစဉ်ဖြစ်ပါသည်။",
            "clean_info_date": "2026-09-20",
            "original_short_note": "ရန်ကုန်၊ စက်တင်ဘာ ၂၀ ရက်",
            "url_lists": json.dumps(["https://example.com/news"]),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    ]
    stage_channel_records(
        stage_dir=stage_dir,
        category="news",
        channel_name="khitthitnews",
        run_date="2026-09-25",
        text_rows=text_rows,
        extra_rows=extra_rows,
        error_rows=[],
    )

    # Verify files created
    news_stage_dir = Path(stage_dir) / "news"
    assert (news_stage_dir / "khitthitnews_2026-09-25_clean_tele_text.jsonl").exists()
    assert (news_stage_dir / "khitthitnews_2026-09-25_clean_tele_extra_info.jsonl").exists()

    # 2. Bulk upload to database
    config = {"postgresql": {"url": pg_url}}
    results = upload_staged_data(config=config, stage_dir=stage_dir, category="news")
    assert results["uploaded_texts"] == 2
    assert results["uploaded_extras"] == 1

    # Verify staging files unlinked
    assert not (news_stage_dir / "khitthitnews_2026-09-25_clean_tele_text.jsonl").exists()

    # Verify database contents
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        texts = session.query(CleanTeleText).all()
        assert len(texts) == 2
        assert texts[0].category == "news"

        extras = session.query(CleanTeleExtraInfo).all()
        assert len(extras) == 1
        assert extras[0].headline == "သတင်းခေါင်းစဉ်ဖြစ်ပါသည်။"
        assert extras[0].clean_info_date == "2026-09-20"
        assert extras[0].original_short_note == "ရန်ကုန်၊ စက်တင်ဘာ ၂၀ ရက်"

        watermarks = session.query(CleaningLog).all()
        assert len(watermarks) == 1
        assert watermarks[0].status == "completed"
        assert watermarks[0].sentences_generated == 2
    finally:
        session.close()


def test_dlq_error_trapping_and_retry(tmp_path):
    db_file = str(tmp_path / "test_dlq.db")
    pg_url = f"sqlite:///{db_file}"
    engine = create_engine(pg_url)
    AnnotationBase.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert an unresolved error record
    err_entry = CleaningErrorLog(
        channel_name="test_channel",
        category="polarization",
        run_date="2026-09-25",
        telegram_message_id=5,
        source_message_id=500,
        raw_text="ပြည်သူများ သတိပြုကြပါရန် အသိပေး နှိုးဆော်အပ်ပါသည် ခင်ဗျား။",
        error_type="SimulationError",
        error_message="Simulated parsing exception",
        resolved=False,
    )
    session.add(err_entry)
    session.commit()
    session.close()

    config = {"postgresql": {"url": pg_url}}
    retry_stats = retry_dlq(config=config, category="polarization")
    assert retry_stats["recovered"] == 1
    assert retry_stats["failed"] == 0

    session = Session()
    try:
        err = session.query(CleaningErrorLog).first()
        assert err.resolved is True
        assert err.resolved_at is not None

        cleaned_rows = session.query(CleanTeleText).all()
        assert len(cleaned_rows) >= 1
        assert cleaned_rows[0].telegram_message_id == 5
    finally:
        session.close()


def test_migration_cleaner_category_backfill(tmp_path, mock_config):
    db_file = str(tmp_path / "test_legacy_cleaner.db")
    engine = create_engine(f"sqlite:///{db_file}")

    # Create legacy tables without category column
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE clean_tele_text (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_message_id INTEGER,
                line_index INTEGER NOT NULL,
                sentence TEXT NOT NULL,
                channel_name VARCHAR(100),
                source_message_id BIGINT,
                created_at TIMESTAMP
            );
        """))
        conn.execute(text("""
            CREATE TABLE cleaning_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name VARCHAR(100) NOT NULL,
                run_date VARCHAR(10) NOT NULL,
                status VARCHAR(20) DEFAULT 'running',
                messages_processed INTEGER DEFAULT 0,
                messages_skipped INTEGER DEFAULT 0,
                sentences_generated INTEGER DEFAULT 0,
                cleaning_start_ts TIMESTAMP,
                cleaning_end_ts TIMESTAMP,
                run_started_at TIMESTAMP
            );
        """))
        # Seed legacy data
        conn.execute(text("""
            INSERT INTO clean_tele_text (telegram_message_id, line_index, sentence, channel_name)
            VALUES (1, 0, 'စာကြောင်းတစ်', '@shweba000'), (2, 0, 'သတင်းစာကြောင်း', 'khitthitnews');
        """))
        conn.execute(text("""
            INSERT INTO cleaning_logs (channel_name, run_date, status)
            VALUES ('shweba000', '2026-09-24', 'completed');
        """))
        conn.commit()

    # Apply migration
    results = apply_annotation_migrations(engine, schema="public", config=mock_config)
    assert "clean_tele_text" in results["columns_added"]
    assert "cleaning_logs" in results["columns_added"]
    assert results["backfilled_clean_text"] >= 2
    assert results["backfilled_cleaning_logs"] >= 1

    # Verify columns and data
    inspector = inspect(engine)
    cols = [c["name"] for c in inspector.get_columns("clean_tele_text")]
    assert "category" in cols

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT channel_name, category FROM clean_tele_text ORDER BY id")).fetchall()
        assert rows[0][1] == "polarization"
        assert rows[1][1] == "news"

        log_row = conn.execute(text("SELECT channel_name, category FROM cleaning_logs")).fetchone()
        assert log_row[1] == "polarization"
