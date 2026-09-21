"""
Airflow DAG: Cold Storage Archival (S3 + Parquet)
==================================================
Scheduled monthly at 02:00 UTC on the 1st of every month.
Executes archival of historical raw messages and unannotated clean text older
than 30 days into AWS S3 Snappy Parquet files and safely purges Neon PostgreSQL.
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
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    }

    with DAG(
        dag_id="telegram_data_archival",
        default_args=default_args,
        description="Archive historical PostgreSQL data to AWS S3 Parquet and purge DB",
        schedule_interval="0 2 1 * *",  # Monthly on the 1st at 02:00 UTC
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "archival", "s3", "parquet", "docker"],
    ) as dag:

        archive_task = DockerOperator(
            task_id="archive_to_s3_parquet",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="archival --table all --retention-days 30",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment={
                "ENVIRONMENT": "{{ var.value.get('ENVIRONMENT', 'prod') }}",
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
                "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
                "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
                "AWS_REGION": "{{ var.value.get('AWS_REGION', 'ap-southeast-1') }}",
                "S3_ARCHIVE_BUCKET_NAME": "{{ var.value.get('S3_ARCHIVE_BUCKET_NAME', 'myanmar-nlp-data-archive') }}",
            },
        )

except ImportError:
    pass
