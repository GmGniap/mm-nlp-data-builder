# Local Server Deployment & Airflow Integration Guide

This document outlines the architecture, containerization workflow, CNCF/OCI registry push process, and Airflow DAG integration for deploying the **Telegram Scraper & Myanmar NLP Cleaner** services to a local/remote self-hosted server.

---

## 🏗️ 1. Overall Deployment Architecture

```mermaid
flowchart TD
    subgraph Local_Machine["💻 Local Dev Machine (Apple Silicon arm64)"]
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
        AirflowDAG["Airflow DAG (DockerOperator / K8s)"]
        DockerDaemon["Remote Docker Engine"]
        MountedConfig["Mounted config.yaml / Airflow Vars"]
        AirflowSecrets["Airflow Variables / Connections<br/>• TELEGRAM_STRING_SESSION<br/>• NEON_DATABASE_URL"]
        
        AirflowDAG -->|Pulls Image| DockerDaemon
        DockerDaemon -->|Reads Config & Secrets| MountedConfig & AirflowSecrets
    end

    subgraph Database["☁️ Cloud Database"]
        NeonDB[("Neon PostgreSQL<br/>• telegram_messages<br/>• clean_tele_text<br/>• scraping_logs")]
    end

    DockerPush -->|Encrypted via Tailscale HTTPS| RegistryHost
    RegistryHost -->|docker pull| DockerDaemon
    DockerDaemon -->|Executes Scraper & Cleaner| NeonDB
```

---

## 🛠️ 2. Dynamic Deployment CLI Script (`deploy_docker.sh`)

The deployment script [`deploy_docker.sh`](file:///Users/thetpaing/Documents/Coding/test_ai_project/deploy_docker.sh) automates building the multi-architecture image locally and pushing it to your private registry over Tailscale.

### Key Features:
- **Zero Hardcoded Secrets**: Reads registry endpoint, image names, and default tags from environment variables or `.env`.
- **Dynamic CLI Arguments**: Override registry, tag, image name, or Dockerfile path on the fly.
- **Apple Silicon Native**: Defaults to `linux/arm64` for Mac M-series servers.

### CLI Usage Reference:

```
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

5. **Deploy to a Different Registry / Host**:
   ```bash
   ./deploy_docker.sh -r 190.165.1.155:5000 -t v0.1
   ```

---

## 🔐 3. Managing Secrets & Configuration on Airflow

### A. Telegram Authentication via `TELEGRAM_STRING_SESSION` (Recommended)
Instead of copying or mounting binary SQLite `.session` files into containers, Telethon supports **String Sessions**:

1. **Generate Session String locally**:
   ```bash
   uv run python services/telegram_scraper/login_telegram.py --string-session
   ```
2. **Save in Airflow**:
   Navigate to **Airflow Web UI ➔ Admin ➔ Variables** and create:
   - Key: `TELEGRAM_STRING_SESSION`
   - Value: *(paste the generated string token)*
   - Key: `NEON_DATABASE_URL`
   - Value: `postgresql://neondb_owner:...@ep-....aws.neon.tech/neondb?sslmode=require`

### B. Mounting Custom `config.yaml` on Airflow
There are two production patterns to update channels, limits, and settings without rebuilding images:

#### Option 1: Host Bind Mount (`DockerOperator`)
Place `config.yaml` in your server's Airflow config directory (e.g., `/opt/airflow/configs/telegram_scraper_config.yaml`).
```python
from docker.types import Mount

mounts = [
    Mount(
        source="/opt/airflow/configs/telegram_scraper_config.yaml",
        target="/app/services/telegram_scraper/config.yaml",
        type="bind",
        read_only=True,
    )
]
```

#### Option 2: Airflow UI Variable (Dynamic Generation)
Store the YAML file content directly in **Airflow Admin ➔ Variables** as `TELEGRAM_SCRAPER_CONFIG_YAML`. A `PythonOperator` writes it to disk right before running the scraper container.

### C. Mounting Custom Myanmar Bigram Dictionary (`1syl.potma.dict`)
The sentence cleaner uses a Dr. Ye Kyaw Thu bigram dictionary (`1syl.potma.dict`) for Myanmar sentence segmentation. To supply or customize this dictionary file at runtime without rebuilding the Docker image:

1. **Docker CLI Mount**:
   ```bash
   docker run --rm \
     -v $(pwd)/services/telegram_scraper/config.yaml:/app/services/telegram_scraper/config.yaml:ro \
     -v $(pwd)/services/telegram_scraper/ref/1syl.potma.dict:/app/services/telegram_scraper/ref/1syl.potma.dict:ro \
     -e NEON_DATABASE_URL="postgresql://..." \
     -e CLEANER_DICT_PATH="/app/services/telegram_scraper/ref/1syl.potma.dict" \
     registry.example.com:5005/telegram_scraper:v0.1 cleaner --lookback 2
   ```

2. **Airflow Mount (`DockerOperator`)**:
   Add a bind `Mount` in the cleaner task:
   ```python
   Mount(
       source="/opt/airflow/ref/1syl.potma.dict",
       target="/app/services/telegram_scraper/ref/1syl.potma.dict",
       type="bind",
       read_only=True,
   )
   ```

---

## 🚀 4. Airflow DAG Implementation Examples

Full working DAG examples are located in [`services/telegram_scraper/airflow_examples/telegram_scraper_dag.py`](file:///Users/thetpaing/Documents/Coding/test_ai_project/services/telegram_scraper/airflow_examples/telegram_scraper_dag.py).

### Example: Production `DockerOperator` DAG

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

REGISTRY_IMAGE = "registry.example.com:5005/telegram_scraper:v0.1"

default_args = {
    "owner": "nlp-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="telegram_pipeline_docker",
    default_args=default_args,
    schedule_interval="0 */6 * * *",  # Run every 6 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["telegram", "scraping", "nlp"],
) as dag:

    # Task 1: Scrape new Telegram messages (last 2 days window)
    scrape_task = DockerOperator(
        task_id="scrape_telegram_messages",
        image=REGISTRY_IMAGE,
        api_version="auto",
        auto_remove=True,
        command="scraper --lookback 2",
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

    # Task 2: Clean scraped text & upload sentences to annotation DB
    clean_task = DockerOperator(
        task_id="clean_myanmar_sentences",
        image=REGISTRY_IMAGE,
        api_version="auto",
        auto_remove=True,
        command="cleaner --lookback 2",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            # Mount custom config.yaml
            Mount(
                source="/opt/airflow/configs/telegram_scraper_config.yaml",
                target="/app/services/telegram_scraper/config.yaml",
                type="bind",
                read_only=True,
            ),
            # Mount custom 1syl.potma.dict bigram dictionary
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

    scrape_task >> clean_task
```

---

## 📋 5. Deployment Checklist

1. [ ] Ensure Tailscale is active on both local dev machine and server (`tailscale status`).
2. [ ] Test Telegram login & extract session string:
   ```bash
   uv run python services/telegram_scraper/login_telegram.py --string-session
   ```
3. [ ] Set `TELEGRAM_STRING_SESSION` and `NEON_DATABASE_URL` in Airflow Variables.
4. [ ] Build & Push Docker image:
   ```bash
   ./deploy_docker.sh -t v0.1 --latest
   ```
5. [ ] Trigger and verify DAG execution in the Airflow Web UI.
