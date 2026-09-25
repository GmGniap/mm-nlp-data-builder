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

    # 5. English dateline with 2-digit day + word month + 4-digit year (e.g. '23 Sep 2026 By MPA')
    raw_eng_dateline_1 = (
        "သတင်းခေါင်းစဉ်အသစ်\n"
        "23 Sep 2026 By MPA\n"
        "သတင်းအပြည့်အစုံ ဖော်ပြချက်ဖြစ်ပါသည်။"
    )
    h_eng1, d_eng1, n_eng1, _, b_eng1 = parse_news_message(raw_eng_dateline_1)
    assert h_eng1 == "သတင်းခေါင်းစဉ်အသစ်"
    assert d_eng1 == "2026-09-23"
    assert n_eng1 == "23 Sep 2026 By MPA"
    assert "23 Sep 2026 By MPA" not in b_eng1
    assert "သတင်းအပြည့်အစုံ ဖော်ပြချက်ဖြစ်ပါသည်။" in b_eng1

    # 6. English dateline with news agency attribution (e.g. '24 Sep 2026 By Khaosod English')
    raw_eng_dateline_2 = (
        "ဘန်ကောက်တွင် ဓားခုတ်မှုဖြစ်ပွား\n"
        "24 Sep 2026 By Khaosod English\n"
        "လူ ၄ ဦး ဒဏ်ရာရရှိခဲ့ကြောင်း သိရသည်။"
    )
    h_eng2, d_eng2, n_eng2, _, b_eng2 = parse_news_message(raw_eng_dateline_2)
    assert h_eng2 == "ဘန်ကောက်တွင် ဓားခုတ်မှုဖြစ်ပွား"
    assert d_eng2 == "2026-09-24"
    assert n_eng2 == "24 Sep 2026 By Khaosod English"
    assert "24 Sep 2026 By Khaosod English" not in b_eng2
    assert "လူ ၄ ဦး ဒဏ်ရာရရှိခဲ့ကြောင်း သိရသည်။" in b_eng2


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


def test_dag_channel_loading_from_config(tmp_path):
    from services.telegram_scraper.airflow_examples.telegram_cleaning_dag import load_channels_from_config
    import yaml

    # Test 1: Modern nested categories format
    config_data = {
        "scraping": {
            "categories": {
                "polarization": {
                    "channels": ["shweba000", "kyawswar49111", "@SittKhwayDead"]
                },
                "news": {
                    "channels": ["@khitthitnews", "theirrawaddy"]
                },
            }
        }
    }
    cfg_file = tmp_path / "test_config.yaml"
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.dump(config_data, f)

    polar_chs, news_chs = load_channels_from_config(cfg_file)
    assert polar_chs == ["shweba000", "kyawswar49111", "@SittKhwayDead"]
    assert news_chs == ["@khitthitnews", "theirrawaddy"]

    # Test 2: Legacy scraping.channels mapping
    legacy_cfg = {
        "scraping": {
            "channels": {
                "polarization": ["ch_polar_1"],
                "news": ["ch_news_1", "ch_news_2"],
            }
        }
    }
    legacy_file = tmp_path / "legacy_config.yaml"
    with open(legacy_file, "w", encoding="utf-8") as f:
        yaml.dump(legacy_cfg, f)

    polar_chs_l, news_chs_l = load_channels_from_config(legacy_file)
    assert polar_chs_l == ["ch_polar_1"]
    assert news_chs_l == ["ch_news_1", "ch_news_2"]

    # Test 3: Missing file returns empty lists gracefully
    missing_polar, missing_news = load_channels_from_config(tmp_path / "non_existent.yaml")
    assert missing_polar == []
    assert missing_news == []


def test_news_extra_info_single_row_per_raw_text(tmp_path):
    """
    Verify that when duplicate raw texts exist (e.g. repeated posts, album media groups),
    clean_tele_extra_info receives exactly 1 row while sentences are properly split into clean_tele_text.
    """
    db_file = str(tmp_path / "test_unique_extra.db")
    engine = create_engine(f"sqlite:///{db_file}")
    ScraperBase.metadata.create_all(engine)
    AnnotationBase.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Raw news post containing 3 lines text (Headline + Dateline + 1 body sentence)
    news_text = (
        "သတင်းခေါင်းစဉ်ဖြစ်ပါသည်။\n"
        "ရန်ကုန်၊ ၂၀၂၆ ခုနှစ်၊ စက်တင်ဘာ ၂၀ ရက်\n"
        "ပြည်သူ့ကာကွယ်ရေးတပ်ဖွဲ့က စစ်ဆင်ရေး ဖော်ဆောင်ခဲ့ကြောင်း သိရသည်။"
    )

    day_start = datetime(2026, 9, 20, 0, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 9, 20, 23, 59, 59, tzinfo=timezone.utc)

    # Insert 3 TelegramMessage rows with identical raw text (e.g. album or reposts)
    for m_id in [101, 102, 103]:
        session.add(TelegramMessage(
            channel_name="khitthitnews",
            category="news",
            message_id=m_id,
            message_text=news_text,
            date=datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc),
            status="pending",
        ))
    session.commit()

    stage_dir = str(tmp_path / "staging")
    stats = _process_channel_day(
        session=session,
        channel_name="khitthitnews",
        run_date="2026-09-20",
        day_start=day_start,
        day_end=day_end,
        bigrams=None,
        dry_run=False,
        force=False,
        category="news",
        stage_dir=stage_dir,
    )

    assert stats["msgs_processed"] == 1
    assert stats["msgs_skipped"] == 2  # The 2 duplicate raw texts are skipped!

    # Check staging JSONL files
    extra_file = Path(stage_dir) / "news" / "khitthitnews_2026-09-20_clean_tele_extra_info.jsonl"
    text_file = Path(stage_dir) / "news" / "khitthitnews_2026-09-20_clean_tele_text.jsonl"

    assert extra_file.exists()
    assert text_file.exists()

    with open(extra_file, "r", encoding="utf-8") as f:
        extra_lines = [l for l in f if l.strip()]
    assert len(extra_lines) == 1  # Exactly ONE line for the raw text!

    with open(text_file, "r", encoding="utf-8") as f:
        text_lines = [l for l in f if l.strip()]
    assert len(text_lines) == 2  # Line 0 (headline) + Line 1 (body sentence)

    # Ingest staged data into database
    cfg = {"postgresql": {"url": f"sqlite:///{db_file}"}}
    res = upload_staged_data(cfg, stage_dir=stage_dir, category="news")
    assert res["uploaded_extras"] == 1
    assert res["uploaded_texts"] == 2

    # Verify database contents
    extras_in_db = session.query(CleanTeleExtraInfo).all()
    assert len(extras_in_db) == 1
    assert extras_in_db[0].headline == "သတင်းခေါင်းစဉ်ဖြစ်ပါသည်။"
    assert extras_in_db[0].message_id == 101

    session.close()


def test_extra_info_staging_and_upload_deduplication(tmp_path):
    """
    Verify that duplicate records in staging files or repeated upload calls
    do not create duplicate clean_tele_extra_info rows in the database.
    """
    stage_dir = str(tmp_path / "staging_dup")
    db_file = str(tmp_path / "test_dup.db")
    engine = create_engine(f"sqlite:///{db_file}")
    AnnotationBase.metadata.create_all(engine)

    extra_rows = [
        {
            "channel_name": "khitthitnews",
            "category": "news",
            "message_id": 501,
            "headline": "သတင်းခေါင်းစဉ် ၅၀၁",
            "clean_info_date": "2026-09-24",
            "original_short_note": "ရန်ကုန်",
            "url_lists": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        # Duplicate record with same message_id
        {
            "channel_name": "khitthitnews",
            "category": "news",
            "message_id": 501,
            "headline": "သတင်းခေါင်းစဉ် ၅၀၁",
            "clean_info_date": "2026-09-24",
            "original_short_note": "ရန်ကုန်",
            "url_lists": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    ]

    # Staging writes unique rows
    stage_channel_records(
        stage_dir=stage_dir,
        category="news",
        channel_name="khitthitnews",
        run_date="2026-09-24",
        text_rows=[],
        extra_rows=extra_rows,
        error_rows=[],
    )

    extra_file = Path(stage_dir) / "news" / "khitthitnews_2026-09-24_clean_tele_extra_info.jsonl"
    with open(extra_file, "r", encoding="utf-8") as f:
        lines = [l for l in f if l.strip()]
    assert len(lines) == 1

    # First upload
    cfg = {"postgresql": {"url": f"sqlite:///{db_file}"}}
    res1 = upload_staged_data(cfg, stage_dir=stage_dir, category="news")
    assert res1["uploaded_extras"] == 1

    Session = sessionmaker(bind=engine)
    session = Session()
    assert session.query(CleanTeleExtraInfo).count() == 1

    # Re-stage and upload again
    stage_channel_records(
        stage_dir=stage_dir,
        category="news",
        channel_name="khitthitnews",
        run_date="2026-09-24",
        text_rows=[],
        extra_rows=extra_rows,
        error_rows=[],
    )
    res2 = upload_staged_data(cfg, stage_dir=stage_dir, category="news")
    assert res2["uploaded_extras"] == 0  # Deduplicated against DB!
    assert session.query(CleanTeleExtraInfo).count() == 1  # Still exactly 1 row!

    session.close()


