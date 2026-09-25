# Skills & Tech-Stack Guidelines

This document lists essential technical skills, libraries, references, and code patterns for AI agents and data engineers operating in this project.

---

## 📋 Prerequisites & Documentation Standards

- Always adhere to [`.agent/rules/doc_standards.md`](file:///Users/thetpaing/Documents/Coding/test_ai_project/.agent/rules/doc_standards.md):
  - Never commit real credentials, passwords, tokens, or live connection strings.
  - Use `<UPPER_SNAKE_CASE>` for environment variables and secrets (e.g. `<NEON_DATABASE_URL>`, `<TELEGRAM_STRING_SESSION>`, `<S3_ARCHIVE_BUCKET>`).
  - Use RFC 5737 dummy IP addresses (e.g. `192.0.2.1`) and fictitious channel handles (e.g. `@sample_news_channel`).
  - Activate the project virtual environment before running commands: `source .venv/bin/activate`.

---

## 🧰 Tech Stack References

### 1. Telegram Scraping Stack
- **Telethon**: Python 3 asyncio Telegram client library.
  - Docs: https://docs.telethon.dev/
  - Best Practice: Always reuse sessions (`StringSession(<TELEGRAM_STRING_SESSION>)`) to avoid re-authentication locks.
  - Handle rate limits with `try...except FloodWaitError as e: await asyncio.sleep(e.seconds)`.
  - Concurrency: Limit concurrent channel scraping to 5 workers using `asyncio.Semaphore(5)`.
- **PyYAML**: YAML parsing library for `config.yaml`.
  - Usage: `config = yaml.safe_load(open('config.yaml'))`.

### 2. Database & Data Engineering Stack
- **SQLAlchemy 2.x**: Object-relational mapping (ORM) and connection pooling.
  - Docs: https://docs.sqlalchemy.org/
  - Best Practice: Use scoped sessions, `bulk_insert_mappings` for batch operations, and pre-ping connection pools (`create_engine(url, pool_pre_ping=True)`).
- **PyArrow & Boto3**: Cold storage Parquet conversion and AWS S3 streaming.
  - Convert data rows to `pyarrow.Table.from_pylist(records)`.
  - Compress using `snappy` or `zstd`.
  - Verify Parquet row count before triggering transactional database purge.

### 3. Workflow Orchestration Stack
- **Apache Airflow 2.8+**: Data pipeline scheduling.
  - DockerOperator for containerized task execution.
  - Dynamic Task Mapping (`.expand()`) for parallel channel workloads.
  - Category-based TaskGroups (`TaskGroup("category_polarization")`).

### 4. Web Framework Stack
- **Flask 2.3+**: Light web application framework.
  - Docs: https://flask.palletsprojects.com/
  - **Flask-Login**: Session management (`@login_required`, `current_user`, `UserMixin`).
  - **Werkzeug Security**: Password hashing (`generate_password_hash`, `check_password_hash`).
  - **Werkzeug ProxyFix**: Reverse proxy header resolution (`x_for=1, x_proto=1, x_host=1`).

### 5. NLP Annotation UI Stack
- **Tailwind CSS**: Utility-first CSS framework.
- **JavaScript Text Range Selector**: `window.getSelection()` for capturing highlighted character offsets in text strings.
- **NLP Format Exports**:
  - **spaCy DocBin / JSON**: `{"text": "...", "entities": [[start, end, "LABEL"]]}`.
  - **HuggingFace Datasets**: `{"tokens": [...], "ner_tags": [...]}`.

---

## ⚡ Useful Code Patterns

### Pattern A: Config Loader & DB Engine Initializer
```python
import os
from pathlib import Path
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def load_config(config_path: str = "config.yaml") -> dict:
    p = Path(config_path)
    if not p.exists():
        p = Path("config.example.yaml")
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def get_db_session(db_uri: str):
    engine = create_engine(db_uri, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    return Session()
```

### Pattern B: PyArrow Parquet Generation & Row Validation
```python
import io
import pyarrow as pa
import pyarrow.parquet as pq

def records_to_parquet_bytes(records: list[dict], compression: str = "snappy") -> bytes:
    """Convert dictionaries to compressed Parquet binary buffer."""
    table = pa.Table.from_pylist(records)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression=compression)
    return sink.getvalue()

def verify_parquet_row_count(parquet_bytes: bytes, expected_count: int) -> bool:
    """Validate that Parquet binary record count exactly matches DB extraction count."""
    reader = pa.BufferReader(parquet_bytes)
    parquet_file = pq.ParquetFile(reader)
    return parquet_file.metadata.num_rows == expected_count
```

### Pattern C: JavaScript Character Offset Capture (for Annotator UI)
```javascript
function getSelectedTextOffsets(containerElement) {
    const selection = window.getSelection();
    if (!selection.rangeCount) return null;
    
    const range = selection.getRangeAt(0);
    const preSelectionRange = range.cloneRange();
    preSelectionRange.selectNodeContents(containerElement);
    preSelectionRange.setEnd(range.startContainer, range.startOffset);
    
    const start = preSelectionRange.toString().length;
    const end = start + range.toString().length;
    
    return {
        text: range.toString(),
        start: start,
        end: end
    };
}
```
