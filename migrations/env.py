import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool, text
from dotenv import load_dotenv
from alembic import context

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env variables
load_dotenv(PROJECT_ROOT / ".env")

# Import models
from shared.annotation_models import AnnotationBase
from shared.scraper_models import ScraperBase

# Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata combining annotation models and scraper models
target_metadata = [AnnotationBase.metadata, ScraperBase.metadata]


def get_db_url() -> str:
    """Resolve database URL from CLI x-args, alembic config, or environment variables."""
    # 1. Check -x db_url=...
    x_args = context.get_x_argument(as_dictionary=True)
    if "db_url" in x_args and x_args["db_url"]:
        url = x_args["db_url"]
    else:
        # 2. Check main option
        main_url = config.get_main_option("sqlalchemy.url")
        if main_url and "driver://user:pass@localhost" not in main_url:
            url = main_url
        else:
            # 3. Check environment variable based on env
            target_env = x_args.get("env", os.getenv("ENVIRONMENT", "dev")).lower()
            if target_env == "prod":
                url = (
                    os.getenv("NEON_DATABASE_URL_PROD")
                    or os.getenv("PROD_DATABASE_URL")
                    or os.getenv("DATABASE_URL_PROD")
                    or os.getenv("NEON_DATABASE_URL")
                    or os.getenv("DATABASE_URL", "")
                )
            elif target_env == "branch":
                branch_name = x_args.get("branch", "")
                branch_key = f"NEON_BRANCH_{branch_name.upper().replace('-', '_')}_URL" if branch_name else ""
                url = (
                    (os.getenv(branch_key) if branch_key else None)
                    or os.getenv("NEON_DATABASE_URL_BRANCH")
                    or os.getenv("DATABASE_URL_BRANCH")
                    or os.getenv("NEON_BRANCH_URL")
                    or os.getenv("NEON_DATABASE_URL")
                    or os.getenv("DATABASE_URL", "")
                )
            else:
                url = (
                    os.getenv("NEON_DATABASE_URL_DEV")
                    or os.getenv("DEV_DATABASE_URL")
                    or os.getenv("DATABASE_URL_DEV")
                    or os.getenv("NEON_DATABASE_URL")
                    or os.getenv("DATABASE_URL", "")
                )

    if not url:
        raise ValueError(
            "Database URL could not be resolved. Please set NEON_DATABASE_URL in .env, "
            "or provide --db-url to the management script."
        )

    # Standardize postgresql URI scheme
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def get_target_schema() -> str:
    """Resolve target schema from CLI x-args or environment."""
    x_args = context.get_x_argument(as_dictionary=True)
    if "schema" in x_args and x_args["schema"]:
        return x_args["schema"]

    target_env = x_args.get("env", os.getenv("ENVIRONMENT", "dev")).lower()
    if target_env == "prod":
        return "production"
    if target_env == "branch" and "branch" in x_args and x_args["branch"]:
        # If branch looks like a schema identifier
        branch_name = x_args["branch"]
        if branch_name.isidentifier():
            return branch_name
    return "public"


def include_object(object, name, type_, reflected, compare_to):
    """
    Safety guardrail for autogenerate:
    Never auto-drop existing database tables. If a table exists in DB but is not
    matched in metadata (e.g. across schema boundaries), ignore it rather than
    generating a destructive DROP TABLE.
    """
    if type_ == "table" and reflected and compare_to is None:
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_db_url()
    schema = get_target_schema()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=schema if schema and schema != "public" else None,
        include_schemas=False,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    url = get_db_url()
    schema = get_target_schema()

    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = url

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    is_postgres = connectable.dialect.name == "postgresql"
    is_sqlite = connectable.dialect.name == "sqlite"

    with connectable.connect() as connection:
        if is_postgres and schema and schema != "public":
            connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            connection.execute(text(f'SET search_path TO "{schema}", public'))
            connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=schema if (is_postgres and schema and schema != "public") else None,
            include_schemas=False,
            include_object=include_object,
            render_as_batch=is_sqlite,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
