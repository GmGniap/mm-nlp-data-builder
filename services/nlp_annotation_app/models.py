"""
services/nlp_annotation_app/models.py
======================================
Flask-SQLAlchemy models for the NLP Annotation Platform.

Architecture
------------
Plain SQLAlchemy (used by cleaner.py) and Flask-SQLAlchemy (used here)
cannot share a declarative base — they use different metaclass systems.
Direct inheritance is therefore not possible.

Instead, this module acts as a *thin extension layer* over the canonical
column definitions in shared/annotation_models.py:

  * Column definitions are imported from shared.annotation_models and
    re-declared on Flask-SQLAlchemy models so there is exactly ONE place
    to change a column — shared/annotation_models.py.  This file only
    adds Flask / app-layer concerns (UserMixin, password helpers,
    db.relationship wiring).

  * CleaningLog is exposed read-only by the Flask app (annotators can
    see cleaning status but never write to it), so it is also mapped here
    from the shared column definitions.

Sync rule
---------
  If you add/remove a column in shared/annotation_models.py you must
  mirror that change here in the corresponding model class.  The
  columns are grouped and labelled so the diff is obvious.

Tables owned / managed:
  - users              annotator accounts
  - clean_tele_text    cleaned sentence lines (produced by cleaner.py)
  - cleaning_logs      cleaner watermark / audit trail  (read-only here)
  - annotation_results submitted annotation JSON blobs
  - skipped_records    skip markers per sentence/user/type
"""

import os
import sys

from datetime import datetime
from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

# ---------------------------------------------------------------------------
# Import canonical column definitions from shared/annotation_models.py
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../../"))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from shared.annotation_models import (
    User             as _SharedUser,
    CleanTeleText    as _SharedCleanTeleText,
    CleaningLog      as _SharedCleaningLog,
    AnnotationResult as _SharedAnnotationResult,
    SkippedRecord    as _SharedSkippedRecord,
)

# ---------------------------------------------------------------------------
# Flask-SQLAlchemy db instance
# ---------------------------------------------------------------------------
db = SQLAlchemy()


# ---------------------------------------------------------------------------
# Helper: extract column kwargs from a shared SQLAlchemy Column so we can
# re-declare it on a Flask-SQLAlchemy model without duplicating the spec.
# ---------------------------------------------------------------------------
def _col(shared_model, attr_name):
    """
    Return a new db.Column that mirrors the column declared on *shared_model*
    for the given *attr_name*.

    We copy the column type and key constraints (primary_key, nullable,
    unique, index, default, autoincrement) so the Flask-SQLAlchemy model
    stays in sync with the canonical definition automatically.
    """
    col = shared_model.__table__.c[attr_name]
    kwargs = dict(
        primary_key=col.primary_key,
        nullable=col.nullable,
        unique=col.unique,
        index=col.index,
        default=col.default.arg if col.default is not None else None,
        autoincrement=col.autoincrement if col.autoincrement != "auto" else True,
    )
    # Drop None-valued keys so Flask-SQLAlchemy uses its own defaults
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return db.Column(col.type, **kwargs)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(UserMixin, db.Model):
    """
    Annotator account.
    Column spec mirrors shared.annotation_models.User.
    Flask extras: UserMixin, set_password(), check_password().
    """
    __tablename__ = "users"

    # --- Columns (mirrored from shared/annotation_models.py → User) ---
    id            = _col(_SharedUser, "id")
    email         = _col(_SharedUser, "email")
    password_hash = _col(_SharedUser, "password_hash")
    role          = _col(_SharedUser, "role")
    created_at    = _col(_SharedUser, "created_at")

    # --- Flask-app-only relationships ---
    annotation_results = db.relationship(
        "AnnotationResult", back_populates="user", cascade="all, delete-orphan"
    )
    skipped_records = db.relationship(
        "SkippedRecord", back_populates="user", cascade="all, delete-orphan"
    )

    # --- Flask-app-only methods ---
    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User {self.email}>"


class CleanTeleText(db.Model):
    """
    One cleaned, sentence-split line produced by cleaner.py.
    Column spec mirrors shared.annotation_models.CleanTeleText.
    """
    __tablename__ = "clean_tele_text"

    # --- Columns (mirrored from shared/annotation_models.py → CleanTeleText) ---
    id                  = _col(_SharedCleanTeleText, "id")
    telegram_message_id = _col(_SharedCleanTeleText, "telegram_message_id")
    line_index          = _col(_SharedCleanTeleText, "line_index")
    sentence            = _col(_SharedCleanTeleText, "sentence")
    channel_name        = _col(_SharedCleanTeleText, "channel_name")
    source_message_id   = _col(_SharedCleanTeleText, "source_message_id")
    created_at          = _col(_SharedCleanTeleText, "created_at")

    # --- Flask-app-only relationships ---
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

    # --- Columns (mirrored from shared/annotation_models.py → CleaningLog) ---
    id                  = _col(_SharedCleaningLog, "id")
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
            f"<CleaningLog {self.run_date} status={self.status} "
            f"msgs={self.messages_processed} sents={self.sentences_generated}>"
        )


class AnnotationResult(db.Model):
    """
    Submitted annotation for a sentence, stored as a flexible JSON blob.
    Column spec mirrors shared.annotation_models.AnnotationResult.
    """
    __tablename__ = "annotation_results"

    # --- Columns ---
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

    # --- Flask-app-only relationships ---
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

    # --- Columns ---
    id              = _col(_SharedSkippedRecord, "id")
    clean_line_id   = db.Column(
        db.Integer, db.ForeignKey("clean_tele_text.id"), nullable=False, index=True
    )
    user_id         = db.Column(
        db.Integer, db.ForeignKey("users.id"), nullable=False, index=True
    )
    annotation_type = _col(_SharedSkippedRecord, "annotation_type")
    created_at      = _col(_SharedSkippedRecord, "created_at")

    # --- Flask-app-only relationships ---
    clean_line = db.relationship("CleanTeleText", back_populates="skipped_records")
    user       = db.relationship("User", back_populates="skipped_records")

    def __repr__(self) -> str:
        return f"<SkippedRecord line:{self.clean_line_id} user:{self.user_id} type:{self.annotation_type}>"
