# Full-Stack NLP Annotation Platform

This service provides a Web UI for reviewing scraped Telegram messages, performing NLP token/entity annotation, and exporting labeled datasets. This annotation code is initated and inspired from Dr.Ye Kyaw Thu's Arluu Annotation Project.

---

## 📁 Directory Layout

```
.
├── app.py                 # Main Flask application entrypoint
├── models.py              # Application DB models extending shared models
├── templates/             # Jinja2 HTML templates (Tailwind CSS)
│   ├── base.html          # Core layout & navbar
│   ├── login.html         # User sign-in page
│   ├── register.html      # User registration page
│   ├── dashboard.html     # Queue metrics & scraped message list
│   └── annotate.html      # Interactive text selection & NER tagging interface
└── requirements.txt        # Web UI dependencies
```

---

## 🚀 Running the Web Application

1. Activate project environment:
   ```bash
   source "../../environment/bin/activate"
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Launch Flask server:
   ```bash
   python app.py
   ```
4. Access web UI at `http://127.0.0.1:5000`.

---

## 🏷️ Annotator Workflow

1. Register or Log in as an Annotator.
2. View queue status on the **Dashboard**.
3. Open the **Annotate UI**:
   - Highlight any text snippet in a scraped Telegram message using mouse cursor.
   - Choose entity label (`PER`, `ORG`, `LOC`, `TECH`).
   - Click **Save & Next Message** to store annotations in the database and advance to the next pending item.
4. Export dataset anytime via `/export` endpoint in JSON format.
