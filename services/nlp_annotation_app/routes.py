"""
services/nlp_annotation_app/routes.py
======================================
Blueprint for the NLP Annotation feature.
Exposes namespaced endpoints for annotation UI and REST APIs.
"""

from __future__ import annotations

import csv
from datetime import datetime
import io
import json
import os
from pathlib import Path
import yaml

from flask import (
    Blueprint,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import current_user, login_required

from services.extensions import db
from services.nlp_annotation_app.models import (
    AnnotationResult,
    CleanTeleText,
    SkippedRecord,
)

FEATURE_DIR = Path(__file__).resolve().parent

annotation_bp = Blueprint('annotation', __name__, template_folder='templates')

# ---------------------------------------------------------------------------
# Annotation type registry — add new types here in the future
# ---------------------------------------------------------------------------
ANNOTATION_TYPES = [
    {"code": "polarization", "label": "Polarization"},
]
DEFAULT_ANNOTATION_TYPE = "polarization"


def load_annotation_config():
    """Load field configuration from feature-local annotation_config.yaml."""
    config_path = FEATURE_DIR / "annotation_config.yaml"
    if config_path.exists():
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


def get_user_annotator_name() -> str:
    if current_user.is_authenticated and getattr(current_user, 'email', None):
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


def get_sentence_status(clean_line_id: int, user_id: int, annotation_type: str):
    """Determine annotation status for a sentence: 'submitted', 'skipped', or 'pending'."""
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


def get_record_for_clean_line(clean_line, config, index: int, language=None, annotation_type=None):
    """Build an annotation record dict from a CleanTeleText row."""
    if language is None:
        language = config.get("project", {}).get("language", "mya")
    if annotation_type is None:
        annotation_type = DEFAULT_ANNOTATION_TYPE

    if clean_line:
        channel_raw = clean_line.channel_name or ""
        message_id = clean_line.source_message_id or ""
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
        if f.get("type") == "binary":
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


@annotation_bp.app_context_processor
def utility_processor():
    return dict(format_channel_info=format_channel_info)


# ---------------------------------------------------------------------------
# Feature UI Routes
# ---------------------------------------------------------------------------

@annotation_bp.route('/')
@login_required
def annotate():
    return render_template('annotate.html')


@annotation_bp.route('/export_dataset')
@login_required
def export_dataset():
    """Alias for /annotation/api/save — linked from navigation."""
    return redirect(url_for('annotation.api_save', format='csv'))


# ---------------------------------------------------------------------------
# Feature API Routes
# ---------------------------------------------------------------------------

@annotation_bp.route('/api/config')
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


@annotation_bp.route('/api/state')
@login_required
def api_state():
    index_param = request.args.get('index', type=int, default=-1)
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


@annotation_bp.route('/api/submit', methods=['POST'])
@login_required
def api_submit():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    clean_line_id = data.get("clean_line_id")
    ann_type = data.get("annotation_type", DEFAULT_ANNOTATION_TYPE)
    payload = data.get("payload", {})

    if not clean_line_id:
        return jsonify({"error": "clean_line_id is required"}), 400

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

    skip = SkippedRecord.query.filter_by(
        clean_line_id=clean_line_id,
        user_id=current_user.id,
        annotation_type=ann_type,
    ).first()
    if skip:
        db.session.delete(skip)

    db.session.commit()

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


@annotation_bp.route('/api/skip', methods=['POST'])
@login_required
def api_skip():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    clean_line_id = data.get("clean_line_id")
    ann_type = data.get("annotation_type", DEFAULT_ANNOTATION_TYPE)

    if not clean_line_id:
        return jsonify({"error": "clean_line_id is required"}), 400

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


@annotation_bp.route('/api/update', methods=['POST'])
@login_required
def api_update():
    data = request.get_json()
    if not data or 'index' not in data or 'field' not in data:
        return jsonify({'error': 'Invalid payload'}), 400

    index = data['index']
    field = data['field']
    value = data['value']
    ann_type = data.get('annotation_type', DEFAULT_ANNOTATION_TYPE)

    clean_line = CleanTeleText.query.order_by(CleanTeleText.id.asc()).offset(index).first()
    if not clean_line:
        return jsonify({'error': 'Sentence not found'}), 404

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


@annotation_bp.route('/api/navigate', methods=['POST'])
@login_required
def api_navigate():
    data = request.get_json() or {}
    curr = data.get('current_index', 0)
    direction = data.get('direction', 0)
    new_index = curr + direction
    return redirect(url_for('annotation.api_state', index=new_index))


@annotation_bp.route('/api/goto', methods=['POST'])
@login_required
def api_goto():
    data = request.get_json() or {}
    new_index = data.get('index', 0)
    return redirect(url_for('annotation.api_state', index=new_index))


@annotation_bp.route('/api/add', methods=['POST'])
@login_required
def api_add():
    data = request.get_json() or {}
    text_content = data.get('text', 'New sample text record')

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
    return redirect(url_for('annotation.api_state', index=total - 1))


@annotation_bp.route('/api/delete', methods=['POST'])
@login_required
def api_delete():
    data = request.get_json() or {}
    index = data.get('index', 0)

    clean_line = CleanTeleText.query.order_by(CleanTeleText.id.asc()).offset(index).first()
    if clean_line:
        db.session.delete(clean_line)
        db.session.commit()

    total = CleanTeleText.query.count()
    new_idx = min(index, max(0, total - 1))
    return redirect(url_for('annotation.api_state', index=new_idx))


@annotation_bp.route('/api/autosave', methods=['POST'])
@login_required
def api_autosave():
    return jsonify({'ok': True})


@annotation_bp.route('/api/save', methods=['POST', 'GET'])
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
