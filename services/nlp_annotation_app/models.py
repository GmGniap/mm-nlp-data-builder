"""
services/nlp_annotation_app/models.py
======================================
Flask-SQLAlchemy models for the NLP Annotation Platform feature.

Imports central database instance and User model from services.extensions / services.models,
and maps feature-specific tables:
  - clean_tele_text
  - cleaning_logs
  - annotation_results
  - skipped_records
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../../"))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from shared.annotation_models import (
    CleanTeleText    as _SharedCleanTeleText,
    CleaningLog      as _SharedCleaningLog,
    AnnotationResult as _SharedAnnotationResult,
    SkippedRecord    as _SharedSkippedRecord,
)
from services.extensions import db
from services.models import User, _col

# Re-export db and User for backward compatibility
__all__ = [
    "db",
    "User",
    "CleanTeleText",
    "CleaningLog",
    "AnnotationResult",
    "SkippedRecord",
]


class CleanTeleText(db.Model):
    """
    One cleaned, sentence-split line produced by cleaner.py.
    Column spec mirrors shared.annotation_models.CleanTeleText.
    """
    __tablename__ = "clean_tele_text"

    id                  = _col(_SharedCleanTeleText, "id")
    telegram_message_id = _col(_SharedCleanTeleText, "telegram_message_id")
    line_index          = _col(_SharedCleanTeleText, "line_index")
    sentence            = _col(_SharedCleanTeleText, "sentence")
    channel_name        = _col(_SharedCleanTeleText, "channel_name")
    source_message_id   = _col(_SharedCleanTeleText, "source_message_id")
    created_at          = _col(_SharedCleanTeleText, "created_at")

    annotation_results = db.relationship(
        "AnnotationResult", back_populates="clean_line", cascade="all, delete-orphan"
    )
    skipped_records = db.relationship(
        "SkippedRecord", back_populates="clean_line", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<CleanTeleText msg:{self.telegram_message_id} line:{self.line_index}>"


class CleaningLog(db.Model):
    """
    Cleaner pipeline watermark — read-only from the Flask app's perspective.
    Column spec mirrors shared.annotation_models.CleaningLog.
    """
    __tablename__ = "cleaning_logs"

    id                  = _col(_SharedCleaningLog, "id")
    channel_name        = _col(_SharedCleaningLog, "channel_name")
    run_date            = _col(_SharedCleaningLog, "run_date")
    status              = _col(_SharedCleaningLog, "status")
    messages_processed  = _col(_SharedCleaningLog, "messages_processed")
    messages_skipped    = _col(_SharedCleaningLog, "messages_skipped")
    sentences_generated = _col(_SharedCleaningLog, "sentences_generated")
    cleaning_start_ts   = _col(_SharedCleaningLog, "cleaning_start_ts")
    cleaning_end_ts     = _col(_SharedCleaningLog, "cleaning_end_ts")
    run_started_at      = _col(_SharedCleaningLog, "run_started_at")

    def __repr__(self) -> str:
        return (
            f"<CleaningLog {self.channel_name}:{self.run_date} status={self.status} "
            f"msgs={self.messages_processed} sents={self.sentences_generated}>"
        )


class AnnotationResult(db.Model):
    """
    Submitted annotation for a sentence, stored as a flexible JSON blob.
    Column spec mirrors shared.annotation_models.AnnotationResult.
    """
    __tablename__ = "annotation_results"

    id              = _col(_SharedAnnotationResult, "id")
    clean_line_id   = db.Column(
        db.Integer, db.ForeignKey("clean_tele_text.id"), nullable=False, index=True
    )
    user_id         = db.Column(
        db.Integer, db.ForeignKey("users.id"), nullable=False, index=True
    )
    annotation_type = _col(_SharedAnnotationResult, "annotation_type")
    payload_json    = _col(_SharedAnnotationResult, "payload_json")
    created_at      = _col(_SharedAnnotationResult, "created_at")
    updated_at      = _col(_SharedAnnotationResult, "updated_at")

    clean_line = db.relationship("CleanTeleText", back_populates="annotation_results")
    user       = db.relationship("User", back_populates="annotation_results")

    def __repr__(self) -> str:
        return f"<AnnotationResult line:{self.clean_line_id} user:{self.user_id} type:{self.annotation_type}>"


class SkippedRecord(db.Model):
    """
    Marks a sentence as skipped by a user for a given annotation type.
    Column spec mirrors shared.annotation_models.SkippedRecord.
    """
    __tablename__ = "skipped_records"

    id              = _col(_SharedSkippedRecord, "id")
    clean_line_id   = db.Column(
        db.Integer, db.ForeignKey("clean_tele_text.id"), nullable=False, index=True
    )
    user_id         = db.Column(
        db.Integer, db.ForeignKey("users.id"), nullable=False, index=True
    )
    annotation_type = _col(_SharedSkippedRecord, "annotation_type")
    created_at      = _col(_SharedSkippedRecord, "created_at")

    clean_line = db.relationship("CleanTeleText", back_populates="skipped_records")
    user       = db.relationship("User", back_populates="skipped_records")

    def __repr__(self) -> str:
        return f"<SkippedRecord line:{self.clean_line_id} user:{self.user_id} type:{self.annotation_type}>"
