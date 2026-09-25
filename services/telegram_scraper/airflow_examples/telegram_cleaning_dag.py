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
    from airflow import DAG
    from airflow.decorators import task_group
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    # Path resolution for physical staging volume
    AIRFLOW_HOME = Path(
        os.environ.get("AIRFLOW_HOME", Path(__file__).resolve().parent.parent / ".airflow")
    )
    PROJECT_DIR = AIRFLOW_HOME.parent
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

    # Helper to resolve channel list from config or defaults
    POLARIZATION_CHANNELS = ["shweba000", "kyawswar49111", "SittKhwayDead"]
    NEWS_CHANNELS = ["khitthitnews", "theirrawaddy"]

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

        # -------------------------------------------------------------------
        # Category: Polarization TaskGroup
        # -------------------------------------------------------------------
        @task_group(group_id="clean_category_polarization")
        def polarization_group():
            polar_clean_tasks = []
            for ch in POLARIZATION_CHANNELS:
                clean_op = DockerOperator(
                    task_id=f"clean_polarization_{ch}",
                    image="telegram_scraper:latest",
                    api_version="auto",
                    auto_remove=True,
                    command=f"cleaner --category polarization --channel {ch} --yesterday --stage-dir /app/data/clean_staging",
                    docker_url="unix://var/run/docker.sock",
                    network_mode="bridge",
                    mounts=[staging_mount],
                    environment=common_env,
                )
                polar_clean_tasks.append(clean_op)

            upload_polar = DockerOperator(
                task_id="upload_polarization_batch",
                image="telegram_scraper:latest",
                api_version="auto",
                auto_remove=True,
                command="cleaner --category polarization --upload-staged --stage-dir /app/data/clean_staging",
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge",
                mounts=[staging_mount],
                environment=common_env,
            )

            for t in polar_clean_tasks:
                t >> upload_polar

        # -------------------------------------------------------------------
        # Category: News TaskGroup
        # -------------------------------------------------------------------
        @task_group(group_id="clean_category_news")
        def news_group():
            news_clean_tasks = []
            for ch in NEWS_CHANNELS:
                clean_op = DockerOperator(
                    task_id=f"clean_news_{ch}",
                    image="telegram_scraper:latest",
                    api_version="auto",
                    auto_remove=True,
                    command=f"cleaner --category news --channel {ch} --yesterday --stage-dir /app/data/clean_staging",
                    docker_url="unix://var/run/docker.sock",
                    network_mode="bridge",
                    mounts=[staging_mount],
                    environment=common_env,
                )
                news_clean_tasks.append(clean_op)

            upload_news = DockerOperator(
                task_id="upload_news_batch",
                image="telegram_scraper:latest",
                api_version="auto",
                auto_remove=True,
                command="cleaner --category news --upload-staged --stage-dir /app/data/clean_staging",
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge",
                mounts=[staging_mount],
                environment=common_env,
            )

            for t in news_clean_tasks:
                t >> upload_news

        # Trigger both category groups in parallel
        polarization_group()
        news_group()

except ImportError:
    pass
