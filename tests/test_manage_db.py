"""
tests/test_manage_db.py
=======================
Tests for the unified database migration and management script (manage_db.py)
and Alembic migration system.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from sqlalchemy import create_engine, inspect, text, Column, String

from manage_db import (
    mask_db_url,
    resolve_environment,
    build_alembic_config,
    get_current_revision,
    get_tables_in_db,
    handle_migrate,
    handle_status,
    handle_history,
    handle_downgrade,
    handle_reflect,
    handle_stamp,
)
from shared.annotation_models import AnnotationBase
from shared.scraper_models import ScraperBase


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database file for testing migrations."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db_url = f"sqlite:///{db_path}"
    yield db_url
    if os.path.exists(db_path):
        os.remove(db_path)


def test_mask_db_url():
    """Verify that credentials in connection strings are masked for security."""
    raw = "postgresql://my_user:secret_password@ep-example.aws.neon.tech:5432/neondb?sslmode=require"
    masked = mask_db_url(raw)
    assert "secret_password" not in masked
    assert "my_user:****@ep-example.aws.neon.tech:5432" in masked

    # Test empty or invalid
    assert mask_db_url("") == "<EMPTY>"
    assert mask_db_url("sqlite:///app.db") == "sqlite:///app.db"


def test_resolve_environment(monkeypatch):
    """Test resolution of dev, prod, and branch environments."""
    monkeypatch.setenv("NEON_DATABASE_URL", "postgresql://user:pass@host/main")
    monkeypatch.setenv("NEON_DATABASE_URL_PROD", "postgresql://user:pass@host/prod")
    monkeypatch.setenv("NEON_BRANCH_FEATURE_X_URL", "postgresql://user:pass@host/branch-x")

    # 1. Dev
    env, schema, url = resolve_environment(env="dev")
    assert env == "dev"
    assert schema == "public"
    assert url == "postgresql://user:pass@host/main"

    # 2. Prod
    env, schema, url = resolve_environment(env="prod")
    assert env == "prod"
    assert schema == "production"
    assert url == "postgresql://user:pass@host/prod"

    # 3. Branch with named env var
    env, schema, url = resolve_environment(env="branch", branch="feature-x")
    assert env == "branch"
    assert schema == "public"
    assert url == "postgresql://user:pass@host/branch-x"

    # 4. Custom DB URL override
    custom_url = "sqlite:///custom.db"
    env, schema, url = resolve_environment(db_url=custom_url)
    assert url == custom_url


def test_reflect_and_status_on_fresh_db(temp_sqlite_db, capsys):
    """Test reflecting migrations on a fresh database and checking status."""
    args_reflect = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        yes=True,
    )
    handle_reflect(args_reflect)

    # Verify tables created
    engine = create_engine(temp_sqlite_db)
    tables = get_tables_in_db(engine)
    assert "clean_tele_text" in tables
    assert "telegram_messages" in tables
    assert "alembic_version" in tables

    # Verify status reports up-to-date
    args_status = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
    )
    handle_status(args_status)
    captured = capsys.readouterr().out
    assert "🟢 Up to date with repository head" in captured
    assert "Models and database schema are in sync" in captured


def test_reflect_on_existing_unversioned_db(temp_sqlite_db, capsys):
    """
    Test reflecting when the database already has tables created before Alembic.
    Should safely stamp baseline and avoid table collision errors.
    """
    engine = create_engine(temp_sqlite_db)
    AnnotationBase.metadata.create_all(engine)
    ScraperBase.metadata.create_all(engine)

    # Initial state: tables exist, but alembic_version does not
    assert get_current_revision(engine) is None
    assert "clean_tele_text" in get_tables_in_db(engine)

    # Run reflect
    args_reflect = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        yes=True,
    )
    handle_reflect(args_reflect)

    # Should have stamped baseline and updated to head
    assert get_current_revision(engine) is not None
    captured = capsys.readouterr().out
    assert "Stamping database with baseline revision" in captured


def test_downgrade_and_migrate(temp_sqlite_db, capsys):
    """Test rolling back a revision and migrating forward again."""
    # First reflect to head
    args_reflect = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        yes=True,
    )
    handle_reflect(args_reflect)

    engine = create_engine(temp_sqlite_db)
    rev_before = get_current_revision(engine)
    assert rev_before is not None

    # Downgrade 1 step
    args_downgrade = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        steps=1,
        revision=None,
        yes=True,
    )
    handle_downgrade(args_downgrade)
    rev_after_down = get_current_revision(engine)
    assert rev_after_down != rev_before

    # Migrate back to head
    args_migrate = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        revision="head",
        dry_run=False,
        yes=True,
    )
    handle_migrate(args_migrate)
    assert get_current_revision(engine) == rev_before


def test_history_and_stamp(temp_sqlite_db, capsys):
    """Test history display and manual stamping."""
    args_history = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        verbose=False,
    )
    handle_history(args_history)
    captured = capsys.readouterr().out
    assert "0001_initial_schema" in captured

    # Test stamp
    args_stamp = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        revision="0001_initial_schema",
    )
    handle_stamp(args_stamp)
    engine = create_engine(temp_sqlite_db)
    assert get_current_revision(engine) == "0001_initial_schema"


def test_dry_run_migrate(temp_sqlite_db, capsys):
    """Test that dry-run emits SQL and does not modify the database."""
    args_migrate = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        revision="head",
        dry_run=True,
        yes=True,
    )
    handle_migrate(args_migrate)
    captured = capsys.readouterr().out
    assert "DRY-RUN" in captured
    assert "CREATE TABLE clean_tele_text" in captured

    # Verify tables were NOT actually created in DB
    engine = create_engine(temp_sqlite_db)
    assert len(get_tables_in_db(engine)) == 0


def test_makemigrations_empty(temp_sqlite_db):
    """Test generating an empty migration script and cleaning up."""
    from manage_db import handle_makemigrations
    versions_dir = Path("migrations/versions")
    initial_files = set(versions_dir.glob("*.py"))

    args = argparse.Namespace(
        env="dev",
        branch=None,
        schema="public",
        db_url=temp_sqlite_db,
        message="test_custom_migration",
        empty=True,
    )
    handle_makemigrations(args)

    new_files = set(versions_dir.glob("*.py")) - initial_files
    assert len(new_files) == 1
    new_file = list(new_files)[0]
    try:
        content = new_file.read_text()
        assert "test_custom_migration" in content
        assert "def upgrade()" in content
        assert "def downgrade()" in content
    finally:
        if new_file.exists():
            new_file.unlink()

