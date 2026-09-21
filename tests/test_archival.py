import os
import sys
import io
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import pyarrow.parquet as pq
from shared.scraper_models import ScraperBase, ArchivalLog, TelegramMessage
from shared.annotation_models import AnnotationBase, CleanTeleText, AnnotationResult, SkippedRecord
from services.telegram_scraper.storage import StorageHandler
from services.telegram_scraper.archival import (
    build_s3_key,
    records_to_parquet_buffer,
    verify_s3_parquet_row_count,
    archive_clean_text
)


def test_build_s3_key():
    record_date = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)
    key = build_s3_key(
        environment="prod",
        table_name="telegram_messages",
        channel_name="@shweba000",
        category="polarization",
        record_date=record_date
    )
    assert key == "prod/2026/08/polarization/shweba000/telegram_messages-archived-2026-08-15.parquet"


def test_records_to_parquet_buffer():
    records = [
        {"id": 1, "channel_name": "channel_a", "message_text": "Sample 1", "message_id": 101},
        {"id": 2, "channel_name": "channel_b", "message_text": "Sample 2", "message_id": 102},
    ]
    parquet_bytes = records_to_parquet_buffer(records, compression="snappy")
    assert len(parquet_bytes) > 0

    # Read back with PyArrow to verify validity
    reader = pq.ParquetFile(io.BytesIO(parquet_bytes))
    assert reader.metadata.num_rows == 2
    assert reader.metadata.num_columns == 4


def test_verify_s3_parquet_row_count():
    records = [{"id": 1, "text": "A"}, {"id": 2, "text": "B"}]
    parquet_bytes = records_to_parquet_buffer(records)

    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = parquet_bytes
    mock_s3.get_object.return_value = {"Body": mock_body}

    assert verify_s3_parquet_row_count(mock_s3, "test-bucket", "test.parquet", 2) is True
    assert verify_s3_parquet_row_count(mock_s3, "test-bucket", "test.parquet", 5) is False


def test_safe_purge_protects_human_annotations(tmp_path):
    """
    Critical safety test:
    Verify that clean_tele_text rows associated with AnnotationResult are NEVER
    selected for archival or deleted from the database.
    """
    db_file = tmp_path / "test_archival.db"
    db_url = f"sqlite:///{db_file}"

    # Initialize storage and create all tables on the SQLite database
    storage = StorageHandler(pg_url=db_url, schema="public")
    AnnotationBase.metadata.create_all(storage.engine)

    cutoff = datetime(2026, 8, 1, tzinfo=timezone.utc)
    old_date = datetime(2026, 7, 15, tzinfo=timezone.utc)

    # 1. Insert clean text row #1 (will have human annotation)
    row_annotated = CleanTeleText(
        sentence="Annotated Myanmar sentence",
        channel_name="test_chan",
        created_at=old_date
    )
    # 2. Insert clean text row #2 (unannotated - eligible for archival)
    row_unannotated = CleanTeleText(
        sentence="Unannotated sentence",
        channel_name="test_chan",
        created_at=old_date
    )
    storage.session.add_all([row_annotated, row_unannotated])
    storage.session.commit()

    # Add human annotation linked to row #1
    annotation = AnnotationResult(
        clean_line_id=row_annotated.id,
        user_id=1,
        annotation_type="polarization",
        payload_json='{"polarization": 1}'
    )
    # Add skipped record linked to row #2 (skipped records can be purged with clean text)
    skipped = SkippedRecord(
        clean_line_id=row_unannotated.id,
        user_id=2,
        annotation_type="polarization"
    )
    storage.session.add_all([annotation, skipped])
    storage.session.commit()

    # Query safe unarchived rows: row_annotated MUST be excluded!
    safe_candidates = storage.get_unarchived_clean_text_safe(cutoff_date=cutoff)
    assert len(safe_candidates) == 1
    assert safe_candidates[0].id == row_unannotated.id
    assert safe_candidates[0].id != row_annotated.id

    # Delete unannotated row
    deleted = storage.delete_clean_text_by_ids([row_unannotated.id])
    assert deleted == 1

    # Verify that row_annotated and its annotation_results remain in DB
    remaining_clean = storage.session.query(CleanTeleText).all()
    assert len(remaining_clean) == 1
    assert remaining_clean[0].id == row_annotated.id

    remaining_annotations = storage.session.query(AnnotationResult).all()
    assert len(remaining_annotations) == 1
    assert remaining_annotations[0].clean_line_id == row_annotated.id

    # Verify skipped record was cascaded away cleanly
    remaining_skipped = storage.session.query(SkippedRecord).all()
    assert len(remaining_skipped) == 0

    storage.close()


def test_archival_log_lifecycle(tmp_path):
    db_file = tmp_path / "test_logs.db"
    db_url = f"sqlite:///{db_file}"

    storage = StorageHandler(pg_url=db_url, schema="public")
    cutoff = datetime(2026, 8, 1, tzinfo=timezone.utc)

    log_entry = storage.start_archival_log(
        table_name="telegram_messages",
        cutoff_date=cutoff,
        s3_uri="s3://bucket/test.parquet",
        channel_name="test_chan"
    )
    assert log_entry.status == "in_progress"
    assert log_entry.rows_archived == 0

    storage.finish_archival_log(
        log_id=log_entry.id,
        rows_archived=250,
        file_size_bytes=10240,
        status="completed"
    )

    updated_log = storage.session.query(ArchivalLog).filter_by(id=log_entry.id).first()
    assert updated_log.status == "completed"
    assert updated_log.rows_archived == 250
    assert updated_log.file_size_bytes == 10240
    assert updated_log.finished_at is not None

    storage.close()
