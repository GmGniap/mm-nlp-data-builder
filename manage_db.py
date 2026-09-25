#!/usr/bin/env python3
"""
manage_db.py
============
Unified Database Management & Migration CLI for mm-nlp-data-builder.

Powered by Alembic (the official SQLAlchemy migration framework).
Supports seamless migration management across multiple environments
(dev, prod, branch) and PostgreSQL schemas.

Usage:
------
  # Apply all pending migrations to dev (public schema)
  python manage_db.py migrate --env dev

  # Apply migrations to prod (production schema)
  python manage_db.py migrate --env prod

  # Apply migrations to a specific Neon branch or schema
  python manage_db.py migrate --env branch --branch feature-ner

  # Reflect all updates on user selected environment (auto-detects fresh vs existing DB)
  python manage_db.py reflect --env dev
  python manage_db.py reflect --env branch --branch feature-x

  # Autogenerate a new migration after editing SQLAlchemy models
  python manage_db.py makemigrations -m "add retry count to cleaning logs"

  # Inspect migration status and detect model drift
  python manage_db.py status --env dev

  # View migration history
  python manage_db.py history

  # Roll back the last migration
  python manage_db.py downgrade --steps 1 --env dev

  # Preview SQL without applying (dry-run)
  python manage_db.py migrate --env prod --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env file
load_dotenv(PROJECT_ROOT / ".env")

from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine, inspect, text


# ---------------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------------

def mask_db_url(url: str) -> str:
    """Mask credentials in database URL for safe terminal logging."""
    if not url:
        return "<EMPTY>"
    try:
        parts = urlsplit(url)
        if parts.password:
            user = parts.username or ""
            host = parts.hostname or ""
            port = f":{parts.port}" if parts.port else ""
            netloc = f"{user}:****@{host}{port}"
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return url
    except Exception:
        pass
    return "<URL masked>"


def resolve_environment(
    env: str | None = None,
    branch: str | None = None,
    schema: str | None = None,
    db_url: str | None = None,
) -> tuple[str, str, str]:
    """
    Resolve (environment, schema, database_url) based on flags and env vars.
    Returns:
        (resolved_env, resolved_schema, resolved_db_url)
    """
    resolved_env = (env or os.getenv("ENVIRONMENT") or "dev").lower()
    
    # 1. Resolve DB URL
    if db_url:
        resolved_url = db_url
    elif resolved_env == "prod":
        resolved_url = (
            os.getenv("NEON_DATABASE_URL_PROD")
            or os.getenv("PROD_DATABASE_URL")
            or os.getenv("DATABASE_URL_PROD")
            or os.getenv("NEON_DATABASE_URL")
            or os.getenv("DATABASE_URL", "")
        )
    elif resolved_env == "branch":
        # Check branch-specific env var if provided
        branch_key = f"NEON_BRANCH_{branch.upper().replace('-', '_')}_URL" if branch else ""
        resolved_url = (
            (os.getenv(branch_key) if branch_key else None)
            or os.getenv("NEON_DATABASE_URL_BRANCH")
            or os.getenv("DATABASE_URL_BRANCH")
            or os.getenv("NEON_BRANCH_URL")
            or os.getenv("NEON_DATABASE_URL")
            or os.getenv("DATABASE_URL", "")
        )
    else:  # dev
        resolved_url = (
            os.getenv("NEON_DATABASE_URL_DEV")
            or os.getenv("DEV_DATABASE_URL")
            or os.getenv("DATABASE_URL_DEV")
            or os.getenv("NEON_DATABASE_URL")
            or os.getenv("DATABASE_URL", "")
        )

    if not resolved_url:
        raise ValueError(
            "Database URL could not be resolved. Please set NEON_DATABASE_URL in .env, "
            "or specify --db-url on the command line."
        )

    if resolved_url.startswith("postgres://"):
        resolved_url = "postgresql://" + resolved_url[len("postgres://"):]

    # 2. Resolve Schema
    if schema:
        resolved_schema = schema
    elif resolved_env == "prod":
        resolved_schema = os.getenv("SCHEMA_PROD", "production")
    elif resolved_env == "branch" and branch and branch.isidentifier():
        resolved_schema = branch
    else:
        resolved_schema = os.getenv("SCHEMA_DEV", "public")

    return resolved_env, resolved_schema, resolved_url


def build_alembic_config(
    env: str,
    schema: str,
    db_url: str,
    branch: str | None = None,
) -> Config:
    """Instantiate and configure Alembic Config object with dynamic environment parameters."""
    ini_path = PROJECT_ROOT / "alembic.ini"
    cfg = Config(str(ini_path), stdout=sys.stdout)
    cfg.set_section_option("alembic", "sqlalchemy.url", db_url)

    # Pass dynamic variables to migrations/env.py via x_arguments
    cfg.cmd_opts = argparse.Namespace()
    x_args = [f"env={env}", f"schema={schema}", f"db_url={db_url}"]
    if branch:
        x_args.append(f"branch={branch}")
    cfg.cmd_opts.x = x_args

    return cfg


def get_current_revision(engine, schema: str = "public") -> str | None:
    """Inspect the database to get the current alembic revision hash."""
    is_postgres = engine.dialect.name == "postgresql"
    inspector = inspect(engine)
    
    target_schema = schema if (is_postgres and schema and schema != "public") else None
    tables = inspector.get_table_names(schema=target_schema)

    if "alembic_version" not in tables:
        return None

    tbl = f'"{schema}"."alembic_version"' if (is_postgres and schema and schema != "public") else '"alembic_version"'
    try:
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT version_num FROM {tbl} LIMIT 1")).scalar()
            return str(res) if res else None
    except Exception:
        return None


def get_tables_in_db(engine, schema: str = "public") -> list[str]:
    """Inspect tables currently in target schema."""
    is_postgres = engine.dialect.name == "postgresql"
    inspector = inspect(engine)
    target_schema = schema if (is_postgres and schema and schema != "public") else None
    return inspector.get_table_names(schema=target_schema)


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

def handle_migrate(args: argparse.Namespace) -> None:
    """Apply pending migrations to the database."""
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)
    
    if env == "prod" and not args.dry_run and not getattr(args, "yes", False):
        confirm = input("⚠️  Targeting PRODUCTION environment! Apply migrations? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    print("=" * 65)
    print(" 🚀 Database Migration (Alembic Upgrade)")
    print("=" * 65)
    print(f" Environment : {env}")
    print(f" Target Schema: {schema}")
    print(f" Database URL: {mask_db_url(db_url)}")
    print(f" Target Rev  : {args.revision}")
    print(f" Mode        : {'DRY-RUN (SQL only)' if args.dry_run else 'LIVE APPLY'}")
    print("=" * 65)

    cfg = build_alembic_config(env, schema, db_url, args.branch)

    if args.dry_run:
        engine = create_engine(db_url)
        current_rev = get_current_revision(engine, schema=schema) or "base"
        print(f"\n[*] Generating SQL from '{current_rev}' to '{args.revision}'...\n")
        command.upgrade(cfg, f"{current_rev}:{args.revision}", sql=True)
        print("\n[DRY-RUN] Finished generating SQL without modifying database.")
    else:
        command.upgrade(cfg, args.revision)
        engine = create_engine(db_url)
        new_rev = get_current_revision(engine, schema=schema)
        print(f"\n✅ Migration successful! Database schema is now at revision: {new_rev or 'head'}")


def handle_makemigrations(args: argparse.Namespace) -> None:
    """Autogenerate a new migration script based on model changes."""
    if not args.message:
        print("❌ Error: Please provide a migration message with -m / --message", file=sys.stderr)
        sys.exit(1)

    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)
    
    print("=" * 65)
    print(" 📝 Autogenerating Database Migration Script")
    print("=" * 65)
    print(f" Compare DB  : {env} ({mask_db_url(db_url)})")
    print(f" Schema      : {schema}")
    print(f" Description : {args.message}")
    print("=" * 65)

    cfg = build_alembic_config(env, schema, db_url, args.branch)

    if args.empty:
        command.revision(cfg, message=args.message, autogenerate=False)
        print("\n✅ Created empty migration script in migrations/versions/.")
    else:
        command.revision(cfg, message=args.message, autogenerate=True)
        print("\n✅ Autogenerated migration script in migrations/versions/.")
        print("👉 Run 'python manage_db.py status' to review or 'python manage_db.py migrate' to apply.")


def handle_status(args: argparse.Namespace) -> None:
    """Inspect migration status and detect model drift."""
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)
    cfg = build_alembic_config(env, schema, db_url, args.branch)

    engine = create_engine(db_url, pool_pre_ping=True)
    current_rev = get_current_revision(engine, schema=schema)

    script_dir = ScriptDirectory.from_config(cfg)
    heads = script_dir.get_heads()
    head_rev = heads[0] if heads else None

    print("=" * 65)
    print(" 🔍 Database Migration Status")
    print("=" * 65)
    print(f" Environment       : {env}")
    print(f" Target Schema     : {schema}")
    print(f" Database URL      : {mask_db_url(db_url)}")
    print(f" Current DB Rev    : {current_rev or '(No migration applied / unversioned)'}")
    print(f" Repository Head   : {head_rev or '(No migrations exist)'}")

    if current_rev == head_rev and head_rev is not None:
        print(" Status            : 🟢 Up to date with repository head")
    elif not current_rev:
        print(" Status            : 🟡 Not versioned yet (run 'python manage_db.py reflect' to sync)")
    else:
        print(" Status            : 🔴 Migrations pending! Run 'python manage_db.py migrate'")

    # Check for model drift
    print("\n[*] Checking for model drift (unmigrated model changes)...")
    try:
        command.check(cfg)
        print("✅ Models and database schema are in sync. No unmigrated changes detected.")
    except Exception as e:
        err_msg = str(e)
        if "New upgrade operations detected" in err_msg or "Target database is not up to date" in err_msg:
            print("⚠️  Model drift or pending migrations detected!")
            print("    Run 'python manage_db.py makemigrations -m \"<desc>\"' to create a migration,")
            print("    or 'python manage_db.py migrate' to apply pending migrations.")
        else:
            print(f"ℹ️  Check status: {err_msg}")
    print("=" * 65)


def handle_history(args: argparse.Namespace) -> None:
    """Show migration revision history."""
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)
    cfg = build_alembic_config(env, schema, db_url, args.branch)

    print("=" * 65)
    print(" 📜 Migration History")
    print("=" * 65)
    command.history(cfg, verbose=args.verbose)
    print("=" * 65)


def handle_downgrade(args: argparse.Namespace) -> None:
    """Roll back migrations."""
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)

    if env == "prod" and not getattr(args, "yes", False):
        confirm = input("⚠️  WARNING: You are about to DOWNGRADE PRODUCTION database! Proceed? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    cfg = build_alembic_config(env, schema, db_url, args.branch)
    target = args.revision if args.revision else f"-{args.steps}"

    print(f"[*] Rolling back database ({env}, {schema}) to: {target}")
    command.downgrade(cfg, target)
    print("✅ Downgrade completed.")


def handle_reflect(args: argparse.Namespace) -> None:
    """
    Reflect latest model changes onto user selected environment (dev, branch, prod).
    Intelligently handles:
      1. Completely fresh databases -> runs upgrade head
      2. Databases with existing pre-Alembic tables -> stamps baseline then upgrades to head
      3. Databases with existing migrations -> upgrades to head
    """
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)

    if env == "prod" and not getattr(args, "yes", False):
        confirm = input(f"⚠️  Reflecting updates to PRODUCTION ({schema})? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    print("=" * 65)
    print(" 🔄 Reflecting Model Updates on Target Environment")
    print("=" * 65)
    print(f" Environment : {env}")
    print(f" Schema      : {schema}")
    print(f" Database URL: {mask_db_url(db_url)}")
    print("=" * 65)

    engine = create_engine(db_url, pool_pre_ping=True)
    is_postgres = engine.dialect.name == "postgresql"

    # Ensure schema exists in PostgreSQL
    if is_postgres and schema and schema != "public":
        with engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()

    cfg = build_alembic_config(env, schema, db_url, args.branch)
    current_rev = get_current_revision(engine, schema=schema)
    existing_tables = get_tables_in_db(engine, schema=schema)

    # Known project model tables
    core_tables = {"clean_tele_text", "telegram_messages", "users", "cleaning_logs"}

    if current_rev is None and any(tbl in existing_tables for tbl in core_tables):
        # Database already has pre-existing tables, stamp baseline first
        script_dir = ScriptDirectory.from_config(cfg)
        heads = script_dir.get_heads()
        baseline_rev = "0001_initial_schema" if "0001_initial_schema" in [s.revision for s in script_dir.walk_revisions()] else (heads[0] if heads else None)
        
        print(f"[*] Detected existing tables ({len(existing_tables)} found) without version table.")
        if baseline_rev:
            print(f"[*] Stamping database with baseline revision '{baseline_rev}'...")
            command.stamp(cfg, baseline_rev)
            current_rev = baseline_rev

    # Apply any pending migrations up to head
    print("[*] Applying migrations up to repository head...")
    command.upgrade(cfg, "head")

    updated_rev = get_current_revision(engine, schema=schema)
    print(f"\n✅ Target environment '{env}' ({schema}) is now fully up to date!")
    print(f"   Current active revision: {updated_rev or 'head'}")
    print("=" * 65)


def handle_stamp(args: argparse.Namespace) -> None:
    """Manually stamp the database with a specific revision without running DDL."""
    env, schema, db_url = resolve_environment(args.env, args.branch, args.schema, args.db_url)
    cfg = build_alembic_config(env, schema, db_url, args.branch)

    print(f"[*] Stamping database ({env}, {schema}) with revision '{args.revision}'...")
    command.stamp(cfg, args.revision)
    print("✅ Stamped successfully.")


# ---------------------------------------------------------------------------
# CLI Argument Parser
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified Database Migration and Management CLI for mm-nlp-data-builder.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python manage_db.py migrate --env dev
  python manage_db.py migrate --env prod
  python manage_db.py reflect --env branch --branch feature-ner
  python manage_db.py makemigrations -m "add retry count"
  python manage_db.py status --env dev
  python manage_db.py history
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Common parent parser for environment selection
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument(
        "--env",
        choices=["dev", "prod", "branch"],
        default=None,
        help="Target environment ('dev' -> public, 'prod' -> production, 'branch' -> branch schema/URL)",
    )
    env_parser.add_argument(
        "--branch",
        default=None,
        help="Branch name or connection identifier (used when --env branch)",
    )
    env_parser.add_argument(
        "--schema",
        default=None,
        help="Explicitly override PostgreSQL schema",
    )
    env_parser.add_argument(
        "--db-url",
        default=None,
        help="Explicitly override PostgreSQL database connection URL",
    )
    env_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Bypass interactive confirmation prompt (for CI/Airflow scripts)",
    )

    # 1. migrate / upgrade
    p_migrate = subparsers.add_parser("migrate", parents=[env_parser], help="Apply pending migrations to the database")
    p_migrate.add_argument("--revision", default="head", help="Target revision (default: head)")
    p_migrate.add_argument("--dry-run", action="store_true", help="Print SQL statements without applying to database")
    p_migrate.set_defaults(func=handle_migrate)

    # 2. makemigrations / revision
    p_makemigrations = subparsers.add_parser("makemigrations", parents=[env_parser], help="Autogenerate a new migration script from model changes")
    p_makemigrations.add_argument("-m", "--message", required=True, help="Description for the migration")
    p_makemigrations.add_argument("--empty", action="store_true", help="Create an empty migration script")
    p_makemigrations.set_defaults(func=handle_makemigrations)

    # 3. status / check
    p_status = subparsers.add_parser("status", parents=[env_parser], help="Inspect migration status and detect model drift")
    p_status.set_defaults(func=handle_status)

    # 4. reflect / sync
    p_reflect = subparsers.add_parser("reflect", parents=[env_parser], help="Reflect latest updates on user selected environment (dev, branch, prod)")
    p_reflect.set_defaults(func=handle_reflect)

    # 5. history
    p_history = subparsers.add_parser("history", parents=[env_parser], help="Display migration history")
    p_history.add_argument("-v", "--verbose", action="store_true", help="Show verbose migration information")
    p_history.set_defaults(func=handle_history)

    # 6. downgrade / rollback
    p_downgrade = subparsers.add_parser("downgrade", parents=[env_parser], help="Roll back migrations")
    p_downgrade.add_argument("--steps", type=int, default=1, help="Number of migrations to roll back (default: 1)")
    p_downgrade.add_argument("--revision", default=None, help="Target revision hash to roll back to")
    p_downgrade.set_defaults(func=handle_downgrade)

    # 7. stamp
    p_stamp = subparsers.add_parser("stamp", parents=[env_parser], help="Stamp database with a specific revision without running DDL")
    p_stamp.add_argument("--revision", required=True, help="Revision hash to stamp (e.g. head, 0001_initial_schema)")
    p_stamp.set_defaults(func=handle_stamp)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    try:
        args.func(args)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
