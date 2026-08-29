"""
shared/annotation_models.py
============================
SQLAlchemy models for the NLP Annotation platform.

Tables owned by this module (all in Neon PostgreSQL):
  - users              annotator accounts
  - clean_tele_text    cleaned sentence lines produced by cleaner.py
  - cleaning_logs      per-day watermark / audit trail for the cleaner
  - annotation_results per-sentence annotation submissions (JSON payload)
  - skipped_records    per-sentence skip markers

Key design notes
----------------
* CleanTeleText.telegram_message_id is a *logical* reference to
  telegram_messages.id (managed by the scraper).  No DB-level FK
  constraint is added here because the two metadata objects are
  independent; data integrity is maintained by the cleaner pipeline.

* CleanTeleText carries denormalised channel_name and source_message_id
  columns so the Flask app can build Telegram deep-links without ever
  querying telegram_messages.

* AnnotationResult stores submitted annotations as a flexible JSON blob
  in payload_json, keyed by annotation_type.  This allows each annotation
  type (polarization, NER, sentiment, etc.) to have its own field schema
  without requiring schema migrations.

* SkippedRecord tracks sentences a user chose to skip for a given
  annotation type.  Skipped sentences may be reused later for a
  different annotation type.

* CleaningLog mirrors ScrapingLog: one row per calendar day (run_date).
  The cleaner treats a `completed` row as an idempotency watermark and
  skips that day unless --force is supplied.

Usage
-----
    from shared.annotation_models import (
        User, CleanTeleText, CleaningLog, AnnotationResult,
        SkippedRecord, init_annotation_db
    )
    engine = init_annotation_db("postgresql://user:pw@host/db?sslmode=require")

Only import this module in cleaner.py and the Flask annotation app.
The scraper service must NOT import these models.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, DateTime, ForeignKey, Integer, String, Text,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import declarative_base, relationship

AnnotationBase = declarative_base()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(AnnotationBase):
    """Annotator account."""
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True)
    email         = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(256), nullable=False)
    role          = Column(String(20), default="annotator")  # 'admin' | 'annotator'
    created_at    = Column(DateTime, default=datetime.utcnow)

    # Relationships
    annotation_results = relationship("AnnotationResult", back_populates="user",
                                       cascade="all, delete-orphan")
    skipped_records    = relationship("SkippedRecord", back_populates="user",
                                       cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<User {self.email}>"


class CleanTeleText(AnnotationBase):
    """
    One cleaned, sentence-split line produced by cleaner.py.

    Denormalised columns (channel_name, source_message_id) are populated
    by the cleaner so the Flask app can resolve channel info without
    querying telegram_messages.
    """
    __tablename__ = "clean_tele_text"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    # Logical FK to telegram_messages.id (managed by scraper, different metadata)
    telegram_message_id = Column(Integer, nullable=True, index=True)
    line_index          = Column(Integer, nullable=False, default=0)
    sentence            = Column(Text, nullable=False)
    # Denormalised from TelegramMessage for Flask-app self-sufficiency
    channel_name        = Column(String(100), nullable=True, index=True)
    source_message_id   = Column(BigInteger, nullable=True)
    created_at          = Column(DateTime, default=datetime.utcnow)

    # Relationships
    annotation_results = relationship("AnnotationResult", back_populates="clean_line",
                                       cascade="all, delete-orphan")
    skipped_records    = relationship("SkippedRecord", back_populates="clean_line",
                                       cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<CleanTeleText msg:{self.telegram_message_id} line:{self.line_index}>"


class CleaningLog(AnnotationBase):
    """
    Day-level watermark for the cleaner pipeline.

    One row per calendar day (run_date = 'YYYY-MM-DD').  The cleaner
    writes a 'running' row at the start of each day's pass and updates
    it to 'completed' (or 'failed') when done.

    A 'completed' row acts as an idempotency guard: the cleaner skips
    that day entirely on subsequent runs unless --force is supplied,
    which deletes the existing row and all associated CleanTeleText rows
    before re-processing.
    """
    __tablename__ = "cleaning_logs"
    __table_args__ = (
        UniqueConstraint("run_date", name="uq_cleaning_run_date"),
    )

    id                  = Column(Integer, primary_key=True)
    # YYYY-MM-DD date of the TelegramMessage.date window being cleaned
    run_date            = Column(String(10), nullable=False, index=True)
    status              = Column(String(20), nullable=False, default="running")
    # Counters populated at completion
    messages_processed  = Column(Integer, default=0)
    messages_skipped    = Column(Integer, default=0)
    sentences_generated = Column(Integer, default=0)
    # Wall-clock timestamps for the cleaning pass
    cleaning_start_ts   = Column(DateTime, nullable=False)
    cleaning_end_ts     = Column(DateTime, nullable=True)
    # Row lifecycle
    run_started_at      = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<CleaningLog {self.run_date} status={self.status} "
            f"msgs={self.messages_processed} sents={self.sentences_generated}>"
        )


class AnnotationResult(AnnotationBase):
    """
    Submitted annotation for a sentence, stored as a flexible JSON blob.

    Each row represents one user's annotation of one sentence for one
    annotation type.  The payload_json column contains all annotation
    fields as a JSON object — the schema varies by annotation_type:

      - 'polarization': {"polarization": 1, "political": 0, ..., "key_phrase": "..."}
      - Future types will have different JSON shapes.

    Re-submitting the same sentence+user+type UPDATEs the existing row.
    """
    __tablename__ = "annotation_results"
    __table_args__ = (
        UniqueConstraint(
            "clean_line_id", "user_id", "annotation_type",
            name="uq_annotation_result_line_user_type"
        ),
    )

    id              = Column(Integer, primary_key=True)
    clean_line_id   = Column(Integer, ForeignKey("clean_tele_text.id"), nullable=False, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    annotation_type = Column(String(50), nullable=False, index=True)  # e.g. 'polarization'
    payload_json    = Column(Text, nullable=False)  # JSON blob of annotation fields
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, nullable=True)

    # Relationships
    clean_line = relationship("CleanTeleText", back_populates="annotation_results")
    user       = relationship("User", back_populates="annotation_results")

    def __repr__(self) -> str:
        return f"<AnnotationResult line:{self.clean_line_id} user:{self.user_id} type:{self.annotation_type}>"


class SkippedRecord(AnnotationBase):
    """
    Marks a sentence as skipped by a user for a given annotation type.

    Skipped sentences are excluded from the annotation queue for that type
    but may be reused for different annotation types in the future.

    If a user later submits the sentence, the skip record is deleted.
    """
    __tablename__ = "skipped_records"
    __table_args__ = (
        UniqueConstraint(
            "clean_line_id", "user_id", "annotation_type",
            name="uq_skipped_record_line_user_type"
        ),
    )

    id              = Column(Integer, primary_key=True)
    clean_line_id   = Column(Integer, ForeignKey("clean_tele_text.id"), nullable=False, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    annotation_type = Column(String(50), nullable=False, index=True)  # e.g. 'polarization'
    created_at      = Column(DateTime, default=datetime.utcnow)

    # Relationships
    clean_line = relationship("CleanTeleText", back_populates="skipped_records")
    user       = relationship("User", back_populates="skipped_records")

    def __repr__(self) -> str:
        return f"<SkippedRecord line:{self.clean_line_id} user:{self.user_id} type:{self.annotation_type}>"


# ---------------------------------------------------------------------------
# DB initialisation helper
# ---------------------------------------------------------------------------

def init_annotation_db(pg_url: str):
    """
    Connect to Neon PostgreSQL and create annotation-owned tables only
    (users, clean_tele_text, cleaning_logs, annotation_results,
    skipped_records).
    Returns the engine.
    """
    engine = create_engine(pg_url, pool_pre_ping=True)
    AnnotationBase.metadata.create_all(engine)
    return engine
