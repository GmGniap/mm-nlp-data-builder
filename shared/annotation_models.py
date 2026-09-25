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
    BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, Text,
    UniqueConstraint, create_engine, inspect, text,
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
    category            = Column(String(50), nullable=True, index=True)
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
    Day-level watermark for the cleaner pipeline for a specific channel.

    One row per channel and calendar day (run_date = 'YYYY-MM-DD'). The cleaner
    writes a 'running' row at the start of each channel-day's pass and updates
    it to 'completed' (or 'failed') when done.

    A 'completed' row acts as an idempotency guard: the cleaner skips
    that channel-day entirely on subsequent runs unless --force is supplied,
    which deletes the existing row and associated CleanTeleText rows
    before re-processing.
    """
    __tablename__ = "cleaning_logs"
    __table_args__ = (
        UniqueConstraint("channel_name", "run_date", name="uq_cleaning_channel_rundate"),
    )

    id                  = Column(Integer, primary_key=True)
    channel_name        = Column(String(100), nullable=False, index=True)
    category            = Column(String(50), nullable=True, index=True)
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
            f"<CleaningLog {self.channel_name}:{self.run_date} status={self.status} "
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


class CleanTeleExtraInfo(AnnotationBase):
    """
    Metadata for news articles: headline, clean_info_date, original_short_note, external URLs.
    Independent table (no DB-level FK to clean_tele_text) for NER annotation support.
    """
    __tablename__ = "clean_tele_extra_info"

    id                  = Column(Integer, primary_key=True)
    channel_name        = Column(String(100), nullable=False, index=True)
    category            = Column(String(50), nullable=False, index=True)
    message_id          = Column(BigInteger, nullable=False, index=True)
    headline            = Column(Text, nullable=True)
    clean_info_date     = Column(String(100), nullable=True)
    original_short_note = Column(String(255), nullable=True)
    url_lists           = Column(Text, nullable=True)  # JSON-encoded array of URLs
    created_at          = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<CleanTeleExtraInfo ch:{self.channel_name} msg:{self.message_id}>"


class CleaningErrorLog(AnnotationBase):
    """
    DLQ table recording text cleaning failures, error details, and retry status.
    """
    __tablename__ = "cleaning_error_logs"

    id                  = Column(Integer, primary_key=True)
    channel_name        = Column(String(100), nullable=False, index=True)
    category            = Column(String(50), nullable=False, index=True)
    run_date            = Column(String(10), nullable=False, index=True)
    telegram_message_id = Column(Integer, nullable=True)
    source_message_id   = Column(BigInteger, nullable=True)
    raw_text            = Column(Text, nullable=True)
    error_type          = Column(String(100), nullable=False)
    error_message       = Column(Text, nullable=False)
    stack_trace         = Column(Text, nullable=True)
    retry_count         = Column(Integer, default=0)
    resolved            = Column(Boolean, default=False)
    created_at          = Column(DateTime, default=datetime.utcnow)
    resolved_at         = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<CleaningErrorLog ch:{self.channel_name}:{self.run_date} err:{self.error_type}>"


# ---------------------------------------------------------------------------
# Migration & DB initialisation helpers
# ---------------------------------------------------------------------------

def _extract_channel_category_mapping(config: dict | None) -> dict[str, str]:
    """Extract canonical mapping from channel handle to category name."""
    if not config:
        return {}
    scraping_cfg = config.get("scraping", {})
    categories = scraping_cfg.get("categories", {})
    channel_to_category = {}
    if isinstance(categories, dict):
        for cat_name, cat_val in categories.items():
            ch_list = cat_val.get("channels", []) if isinstance(cat_val, dict) else (
                cat_val if isinstance(cat_val, list) else []
            )
            for ch in ch_list:
                ch_str = str(ch).strip()
                bare = ch_str.lstrip("@")
                channel_to_category[ch_str] = str(cat_name)
                channel_to_category[bare] = str(cat_name)
                channel_to_category[f"@{bare}"] = str(cat_name)
    return channel_to_category


def apply_annotation_migrations(engine, schema: str = "public", config: dict | None = None) -> dict:
    """
    Safely apply schema migrations to add 'category' column to clean_tele_text and cleaning_logs.
    Zero-downtime, no table drops, and no data loss.
    Backfills existing rows where category is NULL based on channels defined in config.yaml.
    """
    results = {"columns_added": [], "backfilled_clean_text": 0, "backfilled_cleaning_logs": 0}
    is_postgres = engine.dialect.name == "postgresql"
    inspector = inspect(engine)

    target_tables = ["clean_tele_text", "cleaning_logs"]

    with engine.connect() as conn:
        for tbl in target_tables:
            table_names = inspector.get_table_names(schema=schema if is_postgres else None)
            if tbl not in table_names:
                continue

            columns = [col["name"] for col in inspector.get_columns(tbl, schema=schema if is_postgres else None)]
            if "category" not in columns:
                if is_postgres:
                    qual_tbl = f'"{schema}"."{tbl}"' if (schema and schema != "public") else f'"{tbl}"'
                    conn.execute(text(f'ALTER TABLE {qual_tbl} ADD COLUMN IF NOT EXISTS category VARCHAR(50)'))
                    conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{tbl}_category" ON {qual_tbl} (category)'))
                else:
                    conn.execute(text(f'ALTER TABLE "{tbl}" ADD COLUMN category VARCHAR(50)'))
                    conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{tbl}_category" ON "{tbl}" (category)'))
                results["columns_added"].append(tbl)
        conn.commit()

    # Backfill historical records where category is NULL or empty
    channel_map = _extract_channel_category_mapping(config)
    with engine.connect() as conn:
        for ch, cat in channel_map.items():
            bare = ch.lstrip("@").lower()
            at_bare = f"@{bare}"
            if is_postgres:
                text_tbl = f'"{schema}"."clean_tele_text"' if (schema and schema != "public") else '"clean_tele_text"'
                log_tbl = f'"{schema}"."cleaning_logs"' if (schema and schema != "public") else '"cleaning_logs"'
            else:
                text_tbl = '"clean_tele_text"'
                log_tbl = '"cleaning_logs"'

            res_t = conn.execute(
                text(f"""UPDATE {text_tbl} SET category = :cat
                         WHERE (lower(ltrim(channel_name, '@')) = :bare OR channel_name = :ch OR channel_name = :at_bare)
                         AND (category IS NULL OR category = '')"""),
                {"cat": cat, "bare": bare, "ch": ch, "at_bare": at_bare}
            )
            res_l = conn.execute(
                text(f"""UPDATE {log_tbl} SET category = :cat
                         WHERE (lower(ltrim(channel_name, '@')) = :bare OR channel_name = :ch OR channel_name = :at_bare)
                         AND (category IS NULL OR category = '')"""),
                {"cat": cat, "bare": bare, "ch": ch, "at_bare": at_bare}
            )
            results["backfilled_clean_text"] += res_t.rowcount or 0
            results["backfilled_cleaning_logs"] += res_l.rowcount or 0

        # Fill any remaining unmapped NULL category rows with empty blank value ''
        if is_postgres:
            text_tbl = f'"{schema}"."clean_tele_text"' if (schema and schema != "public") else '"clean_tele_text"'
            log_tbl = f'"{schema}"."cleaning_logs"' if (schema and schema != "public") else '"cleaning_logs"'
        else:
            text_tbl = '"clean_tele_text"'
            log_tbl = '"cleaning_logs"'

        conn.execute(text(f"UPDATE {text_tbl} SET category = '' WHERE category IS NULL"))
        conn.execute(text(f"UPDATE {log_tbl} SET category = '' WHERE category IS NULL"))
        conn.commit()

    return results


def init_annotation_db(pg_url: str, schema: str = "public", config: dict | None = None):
    """
    Connect to Neon PostgreSQL and create annotation-owned tables only
    (users, clean_tele_text, clean_tele_extra_info, cleaning_logs,
    cleaning_error_logs, annotation_results, skipped_records) in the target schema.
    Returns the schema-configured engine.
    """
    engine = create_engine(pg_url, pool_pre_ping=True)
    if engine.dialect.name == "postgresql":
        if schema and schema != "public":
            with engine.connect() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                conn.commit()
        schema_engine = engine.execution_options(schema_translate_map={None: schema})
    else:
        schema_engine = engine
    AnnotationBase.metadata.create_all(schema_engine)
    apply_annotation_migrations(schema_engine, schema=schema, config=config)
    return schema_engine

