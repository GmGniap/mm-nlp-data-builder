"""Main Application entrypoint and factory for Myanmar NLP Platform.

Coordinates core database connection, user authentication, dashboard metrics,
auxiliary microservice orchestration, and feature blueprints.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required, login_user, logout_user
from flask_wtf import FlaskForm
from werkzeug.middleware.proxy_fix import ProxyFix
from wtforms import PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, Length

SERVICES_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SERVICES_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from services.extensions import db, login_manager
from services.models import User, load_user  # registers user_loader
from services.nlp_annotation_app.models import (
    AnnotationResult,
    CleanTeleText,
    CleaningLog,
    SkippedRecord,
)
from services.nlp_annotation_app.routes import annotation_bp, format_channel_info
from services.services_manager import start_recorder_service


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Sign In')


class RegisterForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)])
    submit = SubmitField('Sign Up')


def create_app(test_config: dict | None = None) -> Flask:
    """Application factory for the Myanmar NLP Platform."""
    app = Flask(
        __name__,
        template_folder=str(SERVICES_DIR / "templates"),
    )

    # Base configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-nlp-annotation-secret-key-12345')
    app.config['RECORDING_SERVICE_URL'] = os.getenv('RECORDING_SERVICE_URL', 'http://127.0.0.1:5001/')
    app.config['WTF_CSRF_ENABLED'] = False

    # Database configuration
    db_url = os.getenv('NEON_DATABASE_URL') or os.getenv('DATABASE_URL')
    if not db_url and not test_config:
        raise RuntimeError(
            "No database URL configured. "
            "Set NEON_DATABASE_URL (or DATABASE_URL) in your .env file."
        )

    if db_url:
        if db_url.startswith('postgresql://') or db_url.startswith('postgres://'):
            db_url = 'postgresql+psycopg2://' + db_url.split('://', 1)[1]
        app.config['SQLALCHEMY_DATABASE_URI'] = db_url

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True}

    # Session cookie settings
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = False
    app.config['REMEMBER_COOKIE_SAMESITE'] = 'Lax'

    # Apply test config overrides if provided
    if test_config:
        app.config.update(test_config)

    # Reverse Proxy Pattern readiness: support ProxyFix middleware
    if os.getenv("USE_PROXYFIX", "true").lower() in ("true", "1", "yes"):
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Initialize extensions
    db.init_app(app)
    login_manager.init_app(app)

    # Global template context helpers
    @app.context_processor
    def utility_processor():
        return dict(format_channel_info=format_channel_info)

    # --- Core Portal & Auth Routes ---

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

    @app.route('/logout')
    @login_required
    def logout():
        logout_user()
        return redirect(url_for('login'))

    @app.route('/dashboard')
    @login_required
    def dashboard():
        from sqlalchemy import func
        total_messages = db.session.query(
            func.count(func.distinct(CleanTeleText.telegram_message_id))
        ).scalar() or 0
        total_sentences = CleanTeleText.query.count()

        submitted_sentences = db.session.query(
            func.count(func.distinct(AnnotationResult.clean_line_id))
        ).scalar() or 0
        skipped_sentences = db.session.query(
            func.count(func.distinct(SkippedRecord.clean_line_id))
        ).scalar() or 0
        pending_sentences = total_sentences - submitted_sentences - skipped_sentences
        total_results = AnnotationResult.query.count()

        recent_lines = CleanTeleText.query.order_by(CleanTeleText.id.desc()).limit(5).all()

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

    @app.route('/record')
    def record_audio():
        start_recorder_service(wait_until_ready=True)
        recorder_url = app.config.get('RECORDING_SERVICE_URL', 'http://127.0.0.1:5001/')
        if current_user.is_authenticated and getattr(current_user, 'email', None):
            username_short = current_user.email.split('@')[0]
            sep = '&' if '?' in recorder_url else '?'
            return redirect(f"{recorder_url}{sep}username={username_short}")
        return redirect(recorder_url)

    # --- Backwards Compatibility Redirects ---

    @app.route('/annotate')
    def legacy_annotate():
        return redirect(url_for('annotation.annotate'))

    @app.route('/export_dataset')
    def legacy_export_dataset():
        return redirect(url_for('annotation.export_dataset'))

    # --- Register Feature Blueprints ---
    app.register_blueprint(annotation_bp, url_prefix='/annotation')

    return app


app = create_app()

if __name__ == '__main__':
    with app.app_context():
        db.create_all()

    # In debug mode with Werkzeug reloader, only spawn recorder in parent process
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        start_recorder_service(wait_until_ready=False)

    port = int(os.getenv('PORT', '5002'))
    app.run(debug=True, port=port, host='0.0.0.0')
