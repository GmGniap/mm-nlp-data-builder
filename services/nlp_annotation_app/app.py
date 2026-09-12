import atexit
import csv
from datetime import datetime
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, '../../'))
sys.path.append(PROJECT_ROOT)

from flask import Flask, render_template, redirect, url_for, flash, request, jsonify, Response, send_file
from flask_login import LoginManager, login_user, login_required, logout_user, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, Length
from dotenv import load_dotenv

from services.nlp_annotation_app.models import (
    db, User, CleanTeleText, CleaningLog, AnnotationResult, SkippedRecord
)

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-nlp-annotation-secret-key-12345')
app.config['RECORDING_SERVICE_URL'] = os.getenv(
    'RECORDING_SERVICE_URL', 'http://127.0.0.1:5001/'
)

_recorder_process = None

def get_python_executable() -> str:
    """Find the virtualenv python binary with project dependencies."""
    candidates = [
        os.path.join(sys.prefix, 'bin', 'python'),
        os.path.join(sys.prefix, 'bin', 'python3'),
        os.path.join(os.environ.get('VIRTUAL_ENV', ''), 'bin', 'python'),
        os.path.abspath(os.path.join(BASE_DIR, '../../.venv/bin/python')),
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return sys.executable

def is_port_in_use(port: int = 5001, host: str = '127.0.0.1') -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0

def start_recorder_service(wait_until_ready: bool = False) -> None:
    global _recorder_process
    if is_port_in_use(5001):
        return

    recorder_script = os.path.abspath(
        os.path.join(BASE_DIR, '../recording_app/app.py')
    )
    if not os.path.exists(recorder_script):
        return

    py_exec = get_python_executable()
    print(f"Starting Recorder service on http://127.0.0.1:5001 using {py_exec} ...")
    _recorder_process = subprocess.Popen(
        [py_exec, recorder_script],
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
    )

    def _cleanup():
        global _recorder_process
        if _recorder_process and _recorder_process.poll() is None:
            _recorder_process.terminate()
            try:
                _recorder_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                _recorder_process.kill()

    atexit.register(_cleanup)

    if wait_until_ready:
        for _ in range(30):
            if is_port_in_use(5001):
                break
            time.sleep(0.1)

# Use Neon PostgreSQL — same connection string as scraper/cleaner services.
# Set NEON_DATABASE_URL in your .env file.
_db_url = os.getenv('NEON_DATABASE_URL') or os.getenv('DATABASE_URL')
if not _db_url:
    raise RuntimeError(
        "No database URL configured. "
        "Set NEON_DATABASE_URL (or DATABASE_URL) in your .env file."
    )
# Ensure SQLAlchemy uses the synchronous psycopg2 driver.
if _db_url.startswith('postgresql://') or _db_url.startswith('postgres://'):
    _db_url = 'postgresql+psycopg2://' + _db_url.split('://', 1)[1]
app.config['SQLALCHEMY_DATABASE_URI'] = _db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True}

# Disable CSRF for development; re-enable when going to production
app.config['WTF_CSRF_ENABLED'] = False

# Session cookie settings — keep the cookie alive through POST → redirect
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False  # set True behind HTTPS in production
app.config['REMEMBER_COOKIE_SAMESITE'] = 'Lax'

db.init_app(app)

login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please sign in to continue.'
login_manager.login_message_category = 'info'

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# ---------------------------------------------------------------------------
# Annotation type registry — add new types here in the future
# ---------------------------------------------------------------------------
ANNOTATION_TYPES = [
    {"code": "polarization", "label": "Polarization"},
]
DEFAULT_ANNOTATION_TYPE = "polarization"


# --- Config Management ---
def load_annotation_config():
    config_path = os.path.join(BASE_DIR, "annotation_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    return {
        "project": {"name": "POLAR Telegram Annotation", "language": "mya"},
        "id_pattern": "{language}_{annotator}_{index}",
        "fields": [
            {"name": "id", "type": "auto_id", "readonly": True, "description": "Unique record ID"},
            {"name": "source", "type": "text", "multiline": True, "separator": "|||", "description": "Source URL or channel ID"},
            {"name": "text", "type": "text", "multiline": True, "description": "Main Telegram text"},
            {"name": "key_phrase", "type": "text", "multiline": True, "separator": "|||", "description": "Key phrases separated by |||"},
            {"name": "polarization", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "political", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "racial/ethnic", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "religious", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "gender/sexual", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "other", "type": "binary", "group": "Sub-Task 1 & 2: Polarization Type"},
            {"name": "stereotype", "type": "binary", "group": "Sub-Task 3: Severity"},
            {"name": "vilification", "type": "binary", "group": "Sub-Task 3: Severity"},
            {"name": "dehumanization", "type": "binary", "group": "Sub-Task 3: Severity"},
            {"name": "extreme_language", "type": "binary", "group": "Sub-Task 3: Severity"},
            {"name": "lack_of_empathy", "type": "binary", "group": "Sub-Task 3: Severity"},
            {"name": "invalidation", "type": "binary", "group": "Sub-Task 3: Severity"}
        ]
    }

# --- WTForms ---
class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Sign In')

class RegisterForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)])
    submit = SubmitField('Sign Up')

# --- Helper functions ---
def get_user_annotator_name():
    if current_user.is_authenticated and current_user.email:
        return current_user.email.split('@')[0].lower()
    return "annotator"

def format_channel_info(channel_name, message_id=None):
    """Formats channel ID / handle display name and Telegram deep link."""
    ch_str = str(channel_name).strip().lstrip('@')

    if ch_str.lstrip('-').isdigit():
        clean_id = ch_str.replace('-100', '')
        display_name = f"Channel #{clean_id}"
        source_url = f"https://t.me/c/{clean_id}/{message_id}" if message_id else f"https://t.me/c/{clean_id}"
    else:
        display_name = f"@{ch_str}"
        source_url = f"https://t.me/{ch_str}/{message_id}" if message_id else f"https://t.me/{ch_str}"

    return display_name, source_url

def get_sentence_status(clean_line_id, user_id, annotation_type):
    """
    Determine annotation status for a sentence.
    Returns: 'submitted', 'skipped', or 'pending'
    """
    result = AnnotationResult.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=user_id,
        annotation_type=annotation_type,
    ).first()
    if result:
        return "submitted", result

    skip = SkippedRecord.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=user_id,
        annotation_type=annotation_type,
    ).first()
    if skip:
        return "skipped", None

    return "pending", None

def get_record_for_clean_line(clean_line, config, index, language=None, annotation_type=None):
    """Build an annotation record dict from a CleanTeleText row.

    Source info is read directly from the denormalised columns on
    CleanTeleText (channel_name, source_message_id) — no join to the
    scraper-owned telegram_messages table is needed.

    Args:
        language: override the project default language (e.g. 'eng').
                  Falls back to annotation_config.yaml project.language.
        annotation_type: the current annotation type for status lookup.
    """
    if language is None:
        language = config.get("project", {}).get("language", "mya")
    if annotation_type is None:
        annotation_type = DEFAULT_ANNOTATION_TYPE

    # Derive source info from denormalised CleanTeleText columns
    if clean_line:
        channel_raw = clean_line.channel_name or ""
        message_id  = clean_line.source_message_id or ""
        display_name, source_url = format_channel_info(channel_raw, message_id) if channel_raw else ("--", "")
    else:
        display_name, source_url, channel_raw, message_id = "--", "", "", ""

    auto_id = f"{language}_{message_id}_{index + 1}"
    record = {
        "id": auto_id,
        "db_id": clean_line.id if clean_line else None,
        "source": source_url,
        "channel_display": display_name,
        "channel_raw": channel_raw,
        "message_id": message_id,
        "line_index": clean_line.line_index if clean_line else None,
        "text": clean_line.sentence if clean_line else "",
        "key_phrase": ""
    }

    # Set defaults for binary fields
    for f in config.get("fields", []):
        if f["type"] == "binary":
            record[f["name"]] = "0"

    # Determine status and load saved annotation if submitted
    status = "pending"
    if clean_line and current_user.is_authenticated:
        status, existing_result = get_sentence_status(
            clean_line.id, current_user.id, annotation_type
        )
        if existing_result and existing_result.payload_json:
            try:
                saved_payload = json.loads(existing_result.payload_json)
                record.update(saved_payload)
            except Exception:
                pass

    record["status"] = status
    return record

@app.context_processor
def utility_processor():
    return dict(format_channel_info=format_channel_info)

# --- Routes ---
@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and user.check_password(form.password.data):
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('dashboard'))
        flash('Invalid email or password', 'danger')
    return render_template('login.html', form=form)

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    form = RegisterForm()
    if form.validate_on_submit():
        if User.query.filter_by(email=form.email.data).first():
            flash('Email already registered', 'warning')
            return redirect(url_for('register'))

        user = User(email=form.email.data)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Registration successful! Please sign in.', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', form=form)

@app.route('/dashboard')
@login_required
def dashboard():
    from sqlalchemy import func
    # Count distinct source messages represented in the cleaned sentences
    total_messages = db.session.query(
        func.count(func.distinct(CleanTeleText.telegram_message_id))
    ).scalar() or 0
    total_sentences = CleanTeleText.query.count()

    # Annotation stats based on AnnotationResult and SkippedRecord
    submitted_sentences = db.session.query(
        func.count(func.distinct(AnnotationResult.clean_line_id))
    ).scalar() or 0
    skipped_sentences = db.session.query(
        func.count(func.distinct(SkippedRecord.clean_line_id))
    ).scalar() or 0
    pending_sentences = total_sentences - submitted_sentences - skipped_sentences
    total_results = AnnotationResult.query.count()

    recent_lines = CleanTeleText.query.order_by(CleanTeleText.id.desc()).limit(5).all()

    # Latest cleaning watermark for display
    latest_cleaning = CleaningLog.query.filter_by(status="completed").order_by(
        CleaningLog.run_date.desc()
    ).first()

    return render_template(
        'dashboard.html',
        total_messages=total_messages,
        total_sentences=total_sentences,
        pending_sentences=pending_sentences,
        annotated_sentences=submitted_sentences,
        skipped_sentences=skipped_sentences,
        total_tags=total_results,
        recent_lines=recent_lines,
        latest_cleaning=latest_cleaning,
    )

@app.route('/export_dataset')
@login_required
def export_dataset():
    """Alias for /api/save — linked from the nav bar."""
    return redirect(url_for('api_save', format='csv'))

@app.route('/annotate')
@login_required
def annotate():
    return render_template('annotate.html')


@app.route('/record')
def record_audio():
    start_recorder_service(wait_until_ready=True)
    recorder_url = app.config.get('RECORDING_SERVICE_URL', 'http://127.0.0.1:5001/')
    if current_user.is_authenticated and getattr(current_user, 'email', None):
        username_short = current_user.email.split('@')[0]
        sep = '&' if '?' in recorder_url else '?'
        return redirect(f"{recorder_url}{sep}username={username_short}")
    return redirect(recorder_url)

# --- API Endpoints ---

@app.route('/api/config')
@login_required
def api_config():
    config = load_annotation_config()
    total = CleanTeleText.query.count()
    fields = config.get("fields", [])
    field_names = [f["name"] for f in fields]
    default_language = config.get("project", {}).get("language", "mya")

    return jsonify({
        "annotator": get_user_annotator_name(),
        "fields": fields,
        "field_names": field_names,
        "total": total,
        "project": config.get("project", {}),
        "default_language": default_language,
        "available_languages": [
            {"code": "mya", "label": "Myanmar (mya)"},
            {"code": "eng", "label": "English (eng)"},
        ],
        "default_annotation_type": DEFAULT_ANNOTATION_TYPE,
        "available_annotation_types": ANNOTATION_TYPES,
    })

@app.route('/api/state')
@login_required
def api_state():
    index_param = request.args.get('index', type=int, default=-1)
    # Language and annotation type can be overridden per-request by the UI
    lang_param = request.args.get('lang', default=None)
    ann_type = request.args.get('ann_type', default=DEFAULT_ANNOTATION_TYPE)
    total = CleanTeleText.query.count()

    if total == 0:
        return jsonify({
            "index": 0,
            "total": 0,
            "record": None,
            "annotator": get_user_annotator_name()
        })

    if index_param == -1:
        # Find the first pending sentence for the current user
        subq_ann = db.session.query(AnnotationResult.clean_line_id).filter_by(
            user_id=current_user.id, annotation_type=ann_type
        )
        subq_skip = db.session.query(SkippedRecord.clean_line_id).filter_by(
            user_id=current_user.id, annotation_type=ann_type
        )
        
        first_pending = CleanTeleText.query.filter(
            ~CleanTeleText.id.in_(subq_ann),
            ~CleanTeleText.id.in_(subq_skip)
        ).order_by(CleanTeleText.id.asc()).first()

        if first_pending:
            index_param = CleanTeleText.query.filter(CleanTeleText.id < first_pending.id).count()
        else:
            index_param = total - 1

    if index_param < 0:
        index_param = 0
    if index_param >= total:
        index_param = total - 1

    clean_line = CleanTeleText.query.order_by(CleanTeleText.id.asc()).offset(index_param).first()
    config = load_annotation_config()
    record = get_record_for_clean_line(
        clean_line, config, index_param,
        language=lang_param, annotation_type=ann_type,
    )

    return jsonify({
        "index": index_param,
        "total": total,
        "record": record,
        "annotator": get_user_annotator_name()
    })

@app.route('/api/submit', methods=['POST'])
@login_required
def api_submit():
    """Submit annotation for the current sentence.

    Expects JSON: {
        "clean_line_id": 42,
        "annotation_type": "polarization",
        "payload": { "polarization": 1, "political": 0, ... }
    }

    Upserts the AnnotationResult row. If the sentence was previously
    skipped, removes the skip record.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    clean_line_id = data.get("clean_line_id")
    ann_type = data.get("annotation_type", DEFAULT_ANNOTATION_TYPE)
    payload = data.get("payload", {})

    if not clean_line_id:
        return jsonify({"error": "clean_line_id is required"}), 400

    # Upsert: update if exists, insert otherwise
    existing = AnnotationResult.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=current_user.id,
        annotation_type=ann_type,
    ).first()

    if existing:
        existing.payload_json = json.dumps(payload, ensure_ascii=False)
        existing.updated_at = datetime.utcnow()
    else:
        result = AnnotationResult(
            clean_line_id=clean_line_id,
            user_id=current_user.id,
            annotation_type=ann_type,
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
        db.session.add(result)

    # If previously skipped, remove the skip record
    skip = SkippedRecord.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=current_user.id,
        annotation_type=ann_type,
    ).first()
    if skip:
        db.session.delete(skip)

    db.session.commit()

    # Calculate next index for auto-advance
    total = CleanTeleText.query.count()
    # Find the current sentence's position in the ordered list
    current_line = db.session.get(CleanTeleText, clean_line_id)
    if current_line:
        current_idx = CleanTeleText.query.filter(
            CleanTeleText.id <= clean_line_id
        ).order_by(CleanTeleText.id.asc()).count() - 1
        next_index = min(current_idx + 1, total - 1)
    else:
        next_index = 0

    return jsonify({"ok": True, "next_index": next_index})

@app.route('/api/skip', methods=['POST'])
@login_required
def api_skip():
    """Skip the current sentence for the given annotation type.

    Expects JSON: {
        "clean_line_id": 42,
        "annotation_type": "polarization"
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    clean_line_id = data.get("clean_line_id")
    ann_type = data.get("annotation_type", DEFAULT_ANNOTATION_TYPE)

    if not clean_line_id:
        return jsonify({"error": "clean_line_id is required"}), 400

    # Only skip if not already skipped
    existing = SkippedRecord.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=current_user.id,
        annotation_type=ann_type,
    ).first()

    if not existing:
        skip = SkippedRecord(
            clean_line_id=clean_line_id,
            user_id=current_user.id,
            annotation_type=ann_type,
        )
        db.session.add(skip)
        db.session.commit()

    # Calculate next index for auto-advance
    total = CleanTeleText.query.count()
    current_line = db.session.get(CleanTeleText, clean_line_id)
    if current_line:
        current_idx = CleanTeleText.query.filter(
            CleanTeleText.id <= clean_line_id
        ).order_by(CleanTeleText.id.asc()).count() - 1
        next_index = min(current_idx + 1, total - 1)
    else:
        next_index = 0

    return jsonify({"ok": True, "next_index": next_index})

@app.route('/api/update', methods=['POST'])
@login_required
def api_update():
    """Live-update a single field value (used by inline editing).

    Stores the update in AnnotationResult as part of the payload.
    """
    data = request.get_json()
    if not data or 'index' not in data or 'field' not in data:
        return jsonify({'error': 'Invalid payload'}), 400

    index = data['index']
    field = data['field']
    value = data['value']
    ann_type = data.get('annotation_type', DEFAULT_ANNOTATION_TYPE)

    # Look up the CleanTeleText sentence row
    clean_line = CleanTeleText.query.order_by(CleanTeleText.id.asc()).offset(index).first()
    if not clean_line:
        return jsonify({'error': 'Sentence not found'}), 404

    # Look up or create the AnnotationResult for this sentence + user + type
    result = AnnotationResult.query.filter_by(
        clean_line_id=clean_line.id,
        user_id=current_user.id,
        annotation_type=ann_type,
    ).first()
    if not result:
        result = AnnotationResult(
            clean_line_id=clean_line.id,
            user_id=current_user.id,
            annotation_type=ann_type,
            payload_json=json.dumps({field: value}),
        )
        db.session.add(result)
    else:
        payload = {}
        if result.payload_json:
            try:
                payload = json.loads(result.payload_json)
            except Exception:
                pass
        payload[field] = value
        result.payload_json = json.dumps(payload)
        result.updated_at = datetime.utcnow()

    db.session.commit()
    return jsonify({'ok': True})

@app.route('/api/navigate', methods=['POST'])
@login_required
def api_navigate():
    data = request.get_json()
    curr = data.get('current_index', 0)
    direction = data.get('direction', 0)
    new_index = curr + direction
    return redirect(url_for('api_state', index=new_index))

@app.route('/api/goto', methods=['POST'])
@login_required
def api_goto():
    data = request.get_json()
    new_index = data.get('index', 0)
    return redirect(url_for('api_state', index=new_index))

@app.route('/api/add', methods=['POST'])
@login_required
def api_add():
    data = request.get_json() or {}
    text_content = data.get('text', 'New sample text record')

    # Create a standalone CleanTeleText row (no scraper-DB write).
    new_line = CleanTeleText(
        telegram_message_id=None,
        line_index=0,
        sentence=text_content,
        channel_name="manual_entry",
        source_message_id=None,
    )
    db.session.add(new_line)
    db.session.commit()

    total = CleanTeleText.query.count()
    return redirect(url_for('api_state', index=total - 1))

@app.route('/api/delete', methods=['POST'])
@login_required
def api_delete():
    data = request.get_json()
    index = data.get('index', 0)

    clean_line = CleanTeleText.query.order_by(CleanTeleText.id.asc()).offset(index).first()
    if clean_line:
        db.session.delete(clean_line)
        db.session.commit()

    total = CleanTeleText.query.count()
    new_idx = min(index, max(0, total - 1))
    return redirect(url_for('api_state', index=new_idx))

@app.route('/api/autosave', methods=['POST'])
@login_required
def api_autosave():
    return jsonify({'ok': True})

@app.route('/api/save', methods=['POST', 'GET'])
@login_required
def api_save():
    fmt = request.args.get('format', 'csv')
    config = load_annotation_config()
    fields = config.get("fields", [])
    field_names = [f["name"] for f in fields]

    clean_lines = CleanTeleText.query.order_by(CleanTeleText.id.asc()).all()
    records = []
    for idx, clean_line in enumerate(clean_lines):
        rec = get_record_for_clean_line(clean_line, config, idx)
        records.append(rec)

    if fmt == 'json':
        output = json.dumps(records, ensure_ascii=False, indent=2)
        return Response(output, mimetype='application/json', headers={"Content-Disposition": "attachment;filename=annotations.json"})

    delimiter = "\t" if fmt == "tsv" else ","
    ext = "tsv" if fmt == "tsv" else "csv"

    si = io.StringIO()
    writer = csv.DictWriter(si, fieldnames=field_names, delimiter=delimiter, extrasaction='ignore')
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)

    output = io.BytesIO()
    output.write(si.getvalue().encode('utf-8'))
    output.seek(0)

    return send_file(output, mimetype=f'text/{ext}', as_attachment=True, download_name=f"annotations.{ext}")

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    # In debug mode with Werkzeug reloader, only spawn in the main parent process
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        start_recorder_service(wait_until_ready=False)
    app.run(debug=True, port=5000)
