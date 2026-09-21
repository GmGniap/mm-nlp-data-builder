"""
Airflow DAG Examples for Telegram Scraper Docker Container
===========================================================
This file illustrates 3 standard production patterns to run `telegram_scraper`
in Apache Airflow and dynamically customize `config.yaml`:

Pattern 1: DockerOperator with Host Volume Mount (for Docker-in-Docker / Celery on VM)
Pattern 2: KubernetesPodOperator with ConfigMap & Secret (for Kubernetes/EKS/GKE)
Pattern 3: PythonOperator + DockerOperator using Airflow Variables (UI-Editable Config)
"""

from datetime import datetime, timedelta
from airflow import DAG

# Default DAG arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# PATTERN 1: DockerOperator with Volume Mount
# Best when running Airflow on a server/VM with access to the Docker socket.
# ==============================================================================
try:
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    with DAG(
        dag_id="telegram_scraper_docker_operator",
        default_args=default_args,
        description="Run Telegram Scraper via DockerOperator with Volume Mount",
        schedule_interval="0 */6 * * *",  # every 6 hours
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "scraping", "docker"],
    ) as dag_docker:

        scrape_telegram_task = DockerOperator(
            task_id="scrape_telegram_channels",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="scraper --yesterday",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            # Mount custom config.yaml and session directory from host into container
            mounts=[
                Mount(
                    source="/opt/airflow/configs/telegram_scraper_config.yaml",
                    target="/app/services/telegram_scraper/config.yaml",
                    type="bind",
                    read_only=True,
                ),
                # Optional: mount persistent session file folder if not using TELEGRAM_STRING_SESSION
                Mount(
                    source="/opt/airflow/sessions",
                    target="/app/services/telegram_scraper/data",
                    type="bind",
                ),
            ],
            environment={
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
                "TELEGRAM_STRING_SESSION": "{{ var.value.TELEGRAM_STRING_SESSION }}",
                "TELEGRAM_SESSION_DIR": "/app/services/telegram_scraper/data",
            },
        )

        clean_telegram_task = DockerOperator(
            task_id="clean_telegram_messages",
            image="telegram_scraper:latest",
            api_version="auto",
            auto_remove=True,
            command="cleaner --yesterday",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mounts=[
                # Mount config.yaml
                Mount(
                    source="/opt/airflow/configs/telegram_scraper_config.yaml",
                    target="/app/services/telegram_scraper/config.yaml",
                    type="bind",
                    read_only=True,
                ),
                # Mount custom Myanmar bigram dictionary file
                Mount(
                    source="/opt/airflow/ref/1syl.potma.dict",
                    target="/app/services/telegram_scraper/ref/1syl.potma.dict",
                    type="bind",
                    read_only=True,
                ),
            ],
            environment={
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
                "CLEANER_DICT_PATH": "/app/services/telegram_scraper/ref/1syl.potma.dict",
            },
        )

        scrape_telegram_task >> clean_telegram_task

except ImportError:
    pass


# ==============================================================================
# PATTERN 2: KubernetesPodOperator with ConfigMap & Secret
# Best when Airflow runs on Kubernetes (MWAA, Astronomer, GKE, EKS).
# ==============================================================================
try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    with DAG(
        dag_id="telegram_scraper_k8s_operator",
        default_args=default_args,
        description="Run Telegram Scraper on Kubernetes using ConfigMap mount",
        schedule_interval="0 */6 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "scraping", "k8s"],
    ) as dag_k8s:

        # 1. ConfigMap Volume (Created via `kubectl create configmap telegram-config --from-file=config.yaml`)
        configmap_volume = k8s.V1Volume(
            name="scraper-config-vol",
            config_map=k8s.V1ConfigMapVolumeSource(name="telegram-scraper-config"),
        )
        configmap_mount = k8s.V1VolumeMount(
            name="scraper-config-vol",
            mount_path="/etc/telegram-scraper",
            read_only=True,
        )

        # 2. Secret Environment Variables
        env_vars = [
            k8s.V1EnvVar(
                name="NEON_DATABASE_URL",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="telegram-secrets", key="NEON_DATABASE_URL"
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="TELEGRAM_STRING_SESSION",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="telegram-secrets", key="TELEGRAM_STRING_SESSION"
                    )
                ),
            ),
            # Point container to mounted ConfigMap
            k8s.V1EnvVar(name="CONFIG_PATH", value="/etc/telegram-scraper/config.yaml"),
        ]

        scrape_pod = KubernetesPodOperator(
            task_id="scrape_telegram_pod",
            name="telegram-scraper",
            namespace="airflow",
            image="your-docker-registry/telegram_scraper:latest",
            cmds=["/entrypoint.sh"],
            arguments=["scraper", "--yesterday"],
            volumes=[configmap_volume],
            volume_mounts=[configmap_mount],
            env_vars=env_vars,
            is_delete_operator_pod=True,
            get_logs=True,
        )

        clean_pod = KubernetesPodOperator(
            task_id="clean_telegram_pod",
            name="telegram-cleaner",
            namespace="airflow",
            image="your-docker-registry/telegram_scraper:latest",
            cmds=["/entrypoint.sh"],
            arguments=["cleaner", "--yesterday"],
            volumes=[configmap_volume],
            volume_mounts=[configmap_mount],
            env_vars=env_vars,
            is_delete_operator_pod=True,
            get_logs=True,
        )

        scrape_pod >> clean_pod

except ImportError:
    pass


# ==============================================================================
# PATTERN 3: Airflow Variable (UI-Editable YAML)
# Allows editing channel list directly in the Airflow Web UI without redeploying.
# ==============================================================================
try:
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable
    import tempfile
    import os

    with DAG(
        dag_id="telegram_scraper_ui_variable",
        default_args=default_args,
        description="Generate custom config.yaml from Airflow Variables",
        schedule_interval="0 */6 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telegram", "scraping", "airflow-variable"],
    ) as dag_var:

        def dump_config_from_variable(**context):
            """Reads YAML string from Airflow UI Variable and writes to a shared path."""
            config_content = Variable.get(
                "TELEGRAM_SCRAPER_CONFIG_YAML",
                default_var="""telegram:
  api_id: "YOUR_TELEGRAM_API_ID"
  api_hash: "YOUR_TELEGRAM_API_HASH"
scraping:
  channels:
    - "shweba000"
    - "@kyawswar49111"
  limit_per_channel: 50
""",
            )
            config_path = "/opt/airflow/configs/dynamic_config.yaml"
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(config_content)
            print(f"Custom config written to {config_path}")

        prepare_config = PythonOperator(
            task_id="prepare_custom_config",
            python_callable=dump_config_from_variable,
        )

        # Run DockerOperator mounting the generated /opt/airflow/configs/dynamic_config.yaml
        run_scraper = DockerOperator(
            task_id="run_scraper_dynamic",
            image="telegram_scraper:latest",
            auto_remove=True,
            command="scraper --lookback 2 --config /app/custom_config.yaml",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mounts=[
                Mount(
                    source="/opt/airflow/configs/dynamic_config.yaml",
                    target="/app/custom_config.yaml",
                    type="bind",
                    read_only=True,
                ),
            ],
            environment={
                "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
                "TELEGRAM_STRING_SESSION": "{{ var.value.TELEGRAM_STRING_SESSION }}",
            },
        )

        prepare_config >> run_scraper

except ImportError:
    pass
