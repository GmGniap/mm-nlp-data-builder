"""
Airflow DAG: Telegram Scraper — News Category
=============================================
Scheduled at 06:00 UTC daily (6-hour stagger from Polarization batch).
Runs scraping for the 'news' channel category followed by sentence cleaning
via DockerOperator. Configured with retries=2 (10 min delay) and multi-session rotation.
"""
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.providers.docker.operators.docker import DockerOperator

    default_args = {
        "owner": "nlp-data-engineering",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="telegram_scraper_news",
        default_args=default_args,
        description="Scrape and clean News Telegram channels concurrently",
        schedule="0 6 * * *",  # 06:00 UTC (6h stagger)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "news", "scraper", "docker"],
    ) as dag:

        scrape_task = DockerOperator(
            task_id="scrape_news_channels",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="scraper --category news --yesterday",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment={
                "ENVIRONMENT": "{{ var.value.get('ENVIRONMENT', 'prod') }}",
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
                "TELEGRAM_STRING_SESSION_1": "{{ var.value.get('TELEGRAM_STRING_SESSION_1', '') }}",
                "TELEGRAM_STRING_SESSION_2": "{{ var.value.get('TELEGRAM_STRING_SESSION_2', '') }}",
                "TELEGRAM_STRING_SESSION": "{{ var.value.get('TELEGRAM_STRING_SESSION', '') }}",
            },
        )

        clean_task = DockerOperator(
            task_id="clean_news_messages",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="cleaner --yesterday",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment={
                "ENVIRONMENT": "{{ var.value.get('ENVIRONMENT', 'prod') }}",
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
            },
        )

        scrape_task >> clean_task

except ImportError:
    pass
