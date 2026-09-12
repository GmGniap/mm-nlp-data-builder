"""Core application database models.

Defines global entities such as User and authentication helpers.
"""

from __future__ import annotations

import os
import sys

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../"))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from shared.annotation_models import User as _SharedUser
from services.extensions import db, login_manager


def _col(shared_model, attr_name):
    """
    Return a new db.Column that mirrors the column declared on *shared_model*
    for the given *attr_name*.
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
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return db.Column(col.type, **kwargs)


class User(UserMixin, db.Model):
    """
    Core User account.
    Column spec mirrors shared.annotation_models.User.
    Flask extras: UserMixin, set_password(), check_password().
    """
    __tablename__ = "users"

    id            = _col(_SharedUser, "id")
    email         = _col(_SharedUser, "email")
    password_hash = _col(_SharedUser, "password_hash")
    role          = _col(_SharedUser, "role")
    created_at    = _col(_SharedUser, "created_at")

    # Feature relationships (resolved when models are registered)
    annotation_results = db.relationship(
        "AnnotationResult", back_populates="user", cascade="all, delete-orphan", lazy="select"
    )
    skipped_records = db.relationship(
        "SkippedRecord", back_populates="user", cascade="all, delete-orphan", lazy="select"
    )

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email!r} role={self.role!r}>"


@login_manager.user_loader
def load_user(user_id: str | int) -> User | None:
    return db.session.get(User, int(user_id))
