# Local Server Deployment & Airflow Integration Guide

This document outlines the architecture, containerization workflow, CNCF/OCI registry push process, and Airflow DAG integration for deploying the **Telegram Scraper & Myanmar NLP Cleaner** services to a local or remote self-hosted server.

---

## 📋 Prerequisites

Before deploying the containerized pipeline, ensure the following components and credentials are prepared:
- **Docker Engine & CLI:** Docker 24.0+ installed on both local machine and remote Airflow server.
- **Private OCI Registry:** Access to a secure container registry (e.g. self-hosted Harbor, Docker Registry over Tailscale, or private cloud registry).
- **Network Connectivity:** Tailscale or private VPC connectivity between the deployment machine, registry, and Airflow server.
- **Environment Secrets:**
  - `<TELEGRAM_STRING_SESSION>`: Telethon string session token.
  - `<NEON_DATABASE_URL>`: Connection string for Neon PostgreSQL database (`postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>/<DB_NAME>?sslmode=require`).
  - `<AWS_ACCESS_KEY_ID>` / `<AWS_SECRET_ACCESS_KEY>`: S3 credentials for Parquet cold archival.

---

## 🏗️ 1. Overall Deployment Architecture

```mermaid
flowchart TD
    subgraph Local_Machine["💻 Local Dev Machine (Apple Silicon arm64 / x86_64)"]
        Code["Codebase & Dockerfile"]
        DeployScript["deploy_docker.sh"]
        DockerBuild["docker build --platform linux/arm64"]
        DockerPush["docker push"]
        Code --> DeployScript
        DeployScript --> DockerBuild --> DockerPush
    end

    subgraph Registry_Server["🔒 Self-Hosted CNCF / OCI Registry (e.g. Tailscale / Private VPC)"]
        RegistryHost["registry.example.com:5005<br/>Image: telegram_scraper:v0.1"]
    end

    subgraph Airflow_Server["⚙️ Remote Airflow Server"]
        AirflowDAGs["Decoupled Airflow DAGs<br/>• Scraper DAGs (News / Polarization)<br/>• Cleaner DAG (Staging + Barrier Upload)<br/>• Archival DAG (Monthly S3 Purge)"]
        DockerDaemon["Remote Docker Engine"]
        MountedConfig["Mounted config.yaml / Airflow Vars"]
        StagingDir["Mounted Staging Dir<br/>(/opt/airflow/data/clean_staging)"]
        AirflowSecrets["Airflow Variables / Connections<br/>• TELEGRAM_STRING_SESSION<br/>• NEON_DATABASE_URL"]
        
        AirflowDAGs -->|Invokes DockerOperator| DockerDaemon
        DockerDaemon -->|Reads Config & Secrets| MountedConfig & AirflowSecrets
        DockerDaemon <-->|File Staging (.jsonl)| StagingDir
    end

    subgraph Database["☁️ Neon PostgreSQL (Transactional DB)"]
        NeonDB[("Active Tables (30 Days)<br/>• telegram_messages<br/>• scraping_logs & scraping_error_logs<br/>• clean_tele_text<br/>• clean_tele_extra_info<br/>• cleaning_logs & cleaning_error_logs")]
    end

    subgraph ColdStorage["❄️ AWS S3 (Cold Storage Data Lake)"]
        S3Bucket[("Parquet Archives<br/>s3://<S3_ARCHIVE_BUCKET>/...")]
    end

    DockerPush -->|Encrypted via HTTPS| RegistryHost
    RegistryHost -->|docker pull| DockerDaemon
    DockerDaemon -->|Bulk Ingest / Purge| NeonDB
    DockerDaemon -->|Stream Parquet| S3Bucket
```

---

## 🛠️ 2. Dynamic Deployment CLI Script (`deploy_docker.sh`)

The deployment script [`deploy_docker.sh`](file:///Users/thetpaing/Documents/Coding/test_ai_project/deploy_docker.sh) automates building the multi-architecture image locally and pushing it to your private registry over Tailscale.

### Key Features:
- **Zero Hardcoded Secrets**: Reads registry endpoint, image names, and default tags from environment variables or `.env`.
- **Dynamic CLI Arguments**: Override registry, tag, image name, or Dockerfile path on the fly.
- **Apple Silicon Native**: Defaults to `linux/arm64` for Mac M-series servers.

### CLI Usage Reference:

```text
Usage: ./deploy_docker.sh [OPTIONS]

Options:
  -r, --registry REGISTRY    Target registry host & port (default: registry.example.com:5005)
  -i, --image NAME           Image name (default: telegram_scraper)
  -t, --tag TAG              Image tag/version (default: v0.1)
  -f, --file DOCKERFILE      Path to Dockerfile (default: services/telegram_scraper/Dockerfile)
  -c, --context DIR          Docker build context directory (default: project root)
  -p, --platform PLATFORM    Target architecture (default: linux/arm64)
  -l, --latest               Also tag and push as ':latest'
      --no-push              Build image only, skip pushing to registry
  -h, --help                 Display help message and exit
```

### Common CLI Command Examples:

1. **Default Deployment (Build & Push `v0.1`)**:
   ```bash
   ./deploy_docker.sh
   ```

2. **Deploy New Version with `latest` Alias**:
   ```bash
   ./deploy_docker.sh -t v0.2 --latest
   ```

3. **Deploy with Custom Image Name or Custom Dockerfile**:
   ```bash
   ./deploy_docker.sh -i nlp-cleaner -t 1.0.0 -f services/telegram_scraper/Dockerfile
   ```

4. **Build Locally Without Pushing (Dry Run)**:
   ```bash
   ./deploy_docker.sh -t test --no-push
   ```

5. **Deploy to a Specific Target Registry Host**:
   ```bash
   ./deploy_docker.sh -r 192.0.2.1:5000 -t v0.1
   ```

---

## 🔐 3. Managing Secrets & Configuration on Airflow

### A. Telegram Authentication via `TELEGRAM_STRING_SESSION` (Recommended)
Instead of copying or mounting binary SQLite `.session` files into containers, Telethon supports **String Sessions**:

1. **Generate Session String locally**:
   ```bash
   source .venv/bin/activate && python services/telegram_scraper/login_telegram.py --string-session
   ```
2. **Save in Airflow**:
   Navigate to **Airflow Web UI ➔ Admin ➔ Variables** and configure:
   - Key: `TELEGRAM_STRING_SESSION`
   - Value: *(paste `<TELEGRAM_STRING_SESSION_TOKEN>`)*
   - Key: `NEON_DATABASE_URL`
   - Value: `postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>/<DB_NAME>?sslmode=require`

### B. Mounting Custom `config.yaml` on Airflow
To update target channels, limits, and rate-limiting schedules without rebuilding Docker images:

#### Host Bind Mount (`DockerOperator`)
Place `config.yaml` in your server's Airflow config directory (e.g. `/opt/airflow/configs/telegram_scraper_config.yaml`).
```python
from docker.types import Mount

config_mount = Mount(
    source="/opt/airflow/configs/telegram_scraper_config.yaml",
    target="/app/services/telegram_scraper/config.yaml",
    type="bind",
    read_only=True,
)
```

### C. Staging Directory Mount for Cleaner Concurrency (`clean_staging`)
The sentence cleaner generates intermediate `.jsonl` files per channel to avoid overloading PostgreSQL connections during concurrent task execution:

```python
staging_mount = Mount(
    source="/opt/airflow/data/clean_staging",
    target="/app/data/clean_staging",
    type="bind",
    read_only=False,
)
```

### D. Mounting Custom Myanmar Bigram Dictionary (`1syl.potma.dict`)
The sentence cleaner uses the Dr. Ye Kyaw Thu bigram dictionary (`1syl.potma.dict`) for Myanmar sentence segmentation. To supply or customize this file at runtime:

```python
dict_mount = Mount(
    source="/opt/airflow/ref/1syl.potma.dict",
    target="/app/services/telegram_scraper/ref/1syl.potma.dict",
    type="bind",
    read_only=True,
)
```

---

## 🚀 4. Airflow DAG Implementation Examples

Production DAG implementations are partitioned into dedicated workflows:
- [`telegram_scraper_polarization_dag.py`](file:///Users/thetpaing/Documents/Coding/test_ai_project/services/telegram_scraper/airflow_dags/telegram_scraper_polarization_dag.py)
- [`telegram_scraper_news_dag.py`](file:///Users/thetpaing/Documents/Coding/test_ai_project/services/telegram_scraper/airflow_dags/telegram_scraper_news_dag.py)
- [`telegram_cleaning_dag.py`](file:///Users/thetpaing/Documents/Coding/test_ai_project/services/telegram_scraper/airflow_dags/telegram_cleaning_dag.py)

### Example 1: Scraper Category Pipeline (`DockerOperator` with Dynamic Mapping)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

REGISTRY_IMAGE = "registry.example.com:5005/telegram_scraper:v0.1"

default_args = {
    "owner": "nlp-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="telegram_scraper_news_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Staggered 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["telegram", "scraper", "news"],
) as dag:

    # Scrapes news channels for closed day (yesterday) with DLQ capture
    scrape_news_task = DockerOperator(
        task_id="scrape_news_category",
        image=REGISTRY_IMAGE,
        api_version="auto",
        auto_remove=True,
        command="scraper --category news --yesterday",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/opt/airflow/configs/telegram_scraper_config.yaml",
                target="/app/services/telegram_scraper/config.yaml",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
            "TELEGRAM_STRING_SESSION": "{{ var.value.TELEGRAM_STRING_SESSION }}",
        },
    )
```

### Example 2: Decoupled Cleaner Pipeline (File Staging + Bulk Barrier Upload)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

REGISTRY_IMAGE = "registry.example.com:5005/telegram_scraper:v0.1"

with DAG(
    dag_id="telegram_cleaning_pipeline",
    schedule_interval="0 8 * * *",  # Daily 08:00 UTC (after scraping completes)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["telegram", "cleaner", "nlp"],
) as dag:

    mounts = [
        Mount(source="/opt/airflow/configs/telegram_scraper_config.yaml", target="/app/services/telegram_scraper/config.yaml", type="bind", read_only=True),
        Mount(source="/opt/airflow/ref/1syl.potma.dict", target="/app/services/telegram_scraper/ref/1syl.potma.dict", type="bind", read_only=True),
        Mount(source="/opt/airflow/data/clean_staging", target="/app/data/clean_staging", type="bind", read_only=False),
    ]

    # Step 1: Clean news posts and stage to JSONL without holding DB write lock
    stage_news = DockerOperator(
        task_id="stage_news_cleaning",
        image=REGISTRY_IMAGE,
        command="cleaner --category news --yesterday --stage-dir /app/data/clean_staging",
        mounts=mounts,
        environment={
            "NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}",
            "CLEANER_DICT_PATH": "/app/services/telegram_scraper/ref/1syl.potma.dict",
        },
    )

    # Step 2: Single-transaction bulk ingestion barrier
    upload_news = DockerOperator(
        task_id="upload_news_staged",
        image=REGISTRY_IMAGE,
        command="cleaner --upload-staged --category news --stage-dir /app/data/clean_staging",
        mounts=mounts,
        environment={"NEON_DATABASE_URL": "{{ var.value.NEON_DATABASE_URL }}"},
    )

    stage_news >> upload_news
```

---

## 📋 5. Deployment Step-by-Step Checklist

1. [ ] **Verify Network**: Ensure Tailscale or private network is active (`tailscale status`).
2. [ ] **Generate Session**: Extract Telegram StringSession token:
   ```bash
   source .venv/bin/activate && python services/telegram_scraper/login_telegram.py --string-session
   ```
3. [ ] **Configure Airflow Variables**:
   - Set `TELEGRAM_STRING_SESSION` to `<TELEGRAM_STRING_SESSION_TOKEN>`.
   - Set `NEON_DATABASE_URL` to `postgresql://<DB_USER>:<DB_PASSWORD>@<DB_HOST>/<DB_NAME>?sslmode=require`.
4. [ ] **Prepare Staging Directory**: Create `/opt/airflow/data/clean_staging` on server host with read/write permissions.
5. [ ] **Build & Push Image**:
   ```bash
   ./deploy_docker.sh -t v0.1 --latest
   ```
6. [ ] **Apply Migrations**:
   ```bash
   source .venv/bin/activate && python manage_db.py reflect --env prod
   ```
7. [ ] **Verify DAG Execution**: Unpause DAGs in Airflow Web UI and run dry-run validation.
