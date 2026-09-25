"""
Airflow DAG: Myanmar Data Cleaning & Concurrency Pipeline
=========================================================
Scheduled daily at 08:00 UTC (after closed-day scraping batches complete).
Runs category-parallel data cleaning across polarization and news channels:
  1. Channels clean dynamically in parallel with DockerOperator.
  2. Each channel task writes intermediate results to a mounted staging volume
     (PROJECT_DIR/data/clean_staging/<category>/<channel>_<run_date>.jsonl).
  3. When all channel tasks for a category finish, a barrier upload task executes
     a single pooled transaction to Neon PostgreSQL, preventing connection limits.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

# Path resolution for physical staging volume and configuration
AIRFLOW_HOME = Path(
    os.environ.get("AIRFLOW_HOME", Path(__file__).resolve().parent.parent / ".airflow")
)
PROJECT_DIR = AIRFLOW_HOME.parent

# Config path resolution (from environment or default mounted location)
CONFIG_PATH = Path(
    os.environ.get(
        "TELEGRAM_CONFIG_PATH",
        f"{PROJECT_DIR}/config/telegram_scraper_config.yaml",
    )
)
if not CONFIG_PATH.is_file():
    # Check local development and alternate fallback locations
    for candidate in [
        PROJECT_DIR / "config" / "telegram_scraper_config.yml",
        PROJECT_DIR / "config" / "config.yaml",
        PROJECT_DIR / "config" / "config.yml",
        PROJECT_DIR / "services" / "telegram_scraper" / "config.yaml",
        PROJECT_DIR / "services" / "telegram_scraper" / "config.yml",
        Path(__file__).resolve().parent.parent / "config.yaml",
        Path(__file__).resolve().parent.parent / "config.yml",
        Path("/opt/airflow/configs/telegram_scraper_config.yaml"),
    ]:
        if candidate.is_file():
            CONFIG_PATH = candidate
            break

# Helper to resolve channel list from config or defaults
DEFAULT_POLARIZATION_CHANNELS = ["shweba000", "kyawswar49111", "SittKhwayDead"]
DEFAULT_NEWS_CHANNELS = ["khitthitnews", "theirrawaddy"]


def load_channels_from_config(config_file: Path | str) -> tuple[list[str], list[str]]:
    """
    Dynamically extract polarization and news channel lists from YAML config.
    Supports both modern nested categories (scraping.categories.<cat>.channels)
    and flat legacy configurations (scraping.channels.<cat>).
    """
    cfg_path = Path(config_file)
    if not cfg_path.is_file() or yaml is None:
        return [], []

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        scraping_cfg = cfg.get("scraping", {})
        categories_cfg = scraping_cfg.get("categories", {})
        channels_cfg = scraping_cfg.get("channels", {})

        def _get_category_channels(cat_name: str) -> list[str]:
            # Check scraping.categories.<cat>
            if isinstance(categories_cfg, dict) and cat_name in categories_cfg:
                cat_val = categories_cfg[cat_name]
                if isinstance(cat_val, dict):
                    chs = cat_val.get("channels", [])
                    if isinstance(chs, list):
                        return [str(c).strip() for c in chs if c]
                elif isinstance(cat_val, list):
                    return [str(c).strip() for c in cat_val if c]

            # Check scraping.channels.<cat>
            if isinstance(channels_cfg, dict) and cat_name in channels_cfg:
                chs = channels_cfg[cat_name]
                if isinstance(chs, list):
                    return [str(c).strip() for c in chs if c]

            return []

        return _get_category_channels("polarization"), _get_category_channels("news")
    except Exception as exc:
        import logging
        logging.getLogger("airflow.task").warning(
            "Could not load channel lists from config (%s): %s", cfg_path, exc
        )
        return [], []


_cfg_polar, _cfg_news = load_channels_from_config(CONFIG_PATH)
POLARIZATION_CHANNELS = _cfg_polar or DEFAULT_POLARIZATION_CHANNELS
NEWS_CHANNELS = _cfg_news or DEFAULT_NEWS_CHANNELS

try:
    from airflow import DAG
    from airflow.decorators import task_group
    from airflow.providers.docker.operators.docker import DockerOperator
    from airflow.operators.empty import EmptyOperator
    from docker.types import Mount

    STAGING_DIR = PROJECT_DIR / "data" / "clean_staging"
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    default_args = {
        "owner": "nlp-data-engineering",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="telegram_cleaning_concurrency",
        default_args=default_args,
        description="Parallel Myanmar sentence cleaning with file-based staging and category bulk upload",
        schedule="0 8 * * *",  # Daily at 08:00 UTC (after closed-day scrapers finish)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "cleaner", "concurrency", "docker"],
    ) as dag:

        common_env = {
            "ENVIRONMENT": "{{ var.value.get('ENVIRONMENT', 'prod') }}",
            "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
        }

        staging_mount = Mount(
            source=str(STAGING_DIR),
            target="/app/data/clean_staging",
            type="bind",
        )

        config_mount = Mount(
            source=str(CONFIG_PATH),
            target="/app/services/telegram_scraper/config.yaml",
            type="bind",
            read_only=True,
        )

        # Dummy Task - Starting Point
        start_dummy = EmptyOperator(task_id="start")

        # -------------------------------------------------------------------
        # Category: Polarization Cleaning TaskGroup
        # -------------------------------------------------------------------
        @task_group(group_id="clean_category_polarization")
        def clean_polarization_group():
            for ch in POLARIZATION_CHANNELS:
                clean_task_id = ch.lstrip("@").replace("-", "_")
                DockerOperator(
                    task_id=f"clean_polarization_{clean_task_id}",
                    image="telegram_scraper:latest",
                    api_version="auto",
                    auto_remove=True,
                    command=f"cleaner --category polarization --channel {ch} --yesterday --stage-dir /app/data/clean_staging",
                    docker_url="unix://var/run/docker.sock",
                    network_mode="bridge",
                    mounts=[staging_mount, config_mount],
                    environment=common_env,
                )

        # -------------------------------------------------------------------
        # Category: News Cleaning TaskGroup
        # -------------------------------------------------------------------
        @task_group(group_id="clean_category_news")
        def clean_news_group():
            for ch in NEWS_CHANNELS:
                clean_task_id = ch.lstrip("@").replace("-", "_")
                DockerOperator(
                    task_id=f"clean_news_{clean_task_id}",
                    image="telegram_scraper:latest",
                    api_version="auto",
                    auto_remove=True,
                    command=f"cleaner --category news --channel {ch} --yesterday --stage-dir /app/data/clean_staging",
                    docker_url="unix://var/run/docker.sock",
                    network_mode="bridge",
                    mounts=[staging_mount, config_mount],
                    environment=common_env,
                )

        # -------------------------------------------------------------------
        # Barrier: Wait for all channel cleaning across BOTH categories
        # -------------------------------------------------------------------
        all_cleaning_complete = EmptyOperator(
            task_id="all_cleaning_complete",
            trigger_rule="all_success",
        )

        # -------------------------------------------------------------------
        # Category Upload Tasks
        # -------------------------------------------------------------------
        upload_polar = DockerOperator(
            task_id="upload_polarization_batch",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="cleaner --category polarization --upload-staged --stage-dir /app/data/clean_staging",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mounts=[staging_mount, config_mount],
            environment=common_env,
        )

        upload_news = DockerOperator(
            task_id="upload_news_batch",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="cleaner --category news --upload-staged --stage-dir /app/data/clean_staging",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mounts=[staging_mount, config_mount],
            environment=common_env,
        )

        # -------------------------------------------------------------------
        # Dependency Flow
        # -------------------------------------------------------------------
        polar_tg = clean_polarization_group()
        news_tg = clean_news_group()

        start_dummy >> [polar_tg, news_tg] >> all_cleaning_complete >> [upload_polar, upload_news]

except ImportError:
    pass
