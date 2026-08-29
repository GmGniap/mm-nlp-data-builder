# Skills & Tech-Stack Guidelines

This document lists essential technical skills, libraries, references, and code patterns for AI agents operating in this project.

---

## 🧰 Tech Stack References

### 1. Telegram Scraping Stack
- **Telethon**: Python 3 asyncio Telegram client library.
  - Docs: https://docs.telethon.dev/
  - Best Practice: Always reuse sessions (`TelegramClient('session_name', api_id, api_hash)`) to avoid re-authentication locks.
  - Handle rate limits with `try...except FloodWaitError as e: await asyncio.sleep(e.seconds)`.
- **PyYAML**: YAML parsing library for `config.yaml`.
  - Usage: `config = yaml.safe_load(open('config.yaml'))`.

### 2. Database Stack
- **SQLAlchemy 2.x**: Object-relational mapping (ORM).
  - Docs: https://docs.sqlalchemy.org/
  - Best Practice: Use scoped sessions and declarative mapping (`db.Model`).
  - Column Types: `db.String`, `db.Text`, `db.BigInteger`, `db.DateTime`, `db.ForeignKey`.

### 3. Web Framework Stack
- **Flask 2.3+**: Light web application framework.
  - Docs: https://flask.palletsprojects.com/
  - **Flask-Login**: Session management (`@login_required`, `current_user`, `UserMixin`).
  - **Flask-WTF / WTForms**: Form validation & CSRF protection.
  - **Werkzeug Security**: Password hashing (`generate_password_hash`, `check_password_hash`).

### 4. NLP Annotation UI Stack
- **Tailwind CSS**: Utility-first CSS framework (loaded via CDN for fast prototyping).
- **JavaScript Text Range Selector**: `window.getSelection()` for capturing highlighted character offsets in text strings.
- **NLP Format Exports**:
  - **spaCy DocBin / JSON**: `{"text": "...", "entities": [[start, end, "LABEL"]]}`.
  - **HuggingFace Datasets**: `{"tokens": [...], "ner_tags": [...]}`.

---

## ⚡ Useful Code Patterns

### Pattern A: Config Loader & DB Session Handler
```python
import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def load_config(config_path="config.yaml"):
    if not os.path.exists(config_path):
        config_path = "config.example.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_db_session(db_uri):
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    return Session()
```

### Pattern B: JavaScript Character Offset Capture (for Annotator UI)
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
