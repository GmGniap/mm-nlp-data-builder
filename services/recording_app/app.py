"""Flask entrypoint for the browser speech-recording microservice."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
from flask import Flask, Response, jsonify, redirect, render_template, request, send_file
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from services.recording_app.storage import RecordingError, RecordingStore

LOCAL_USER_ID = "local"


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        RECORDING_STORAGE_ROOT=os.getenv(
            "RECORDING_STORAGE_ROOT", str(BASE_DIR / "data")
        ),
        RECORDING_PROMPTS_FILE=os.getenv(
            "RECORDING_PROMPTS_FILE", str(BASE_DIR / "prompts.txt")
        ),
        RECORDING_MAX_DURATION_SECONDS=int(
            os.getenv("RECORDING_MAX_DURATION_SECONDS", "120")
        ),
        RECORDING_MAX_CHUNK_BYTES=int(
            os.getenv("RECORDING_MAX_CHUNK_BYTES", str(1024 * 1024))
        ),
        RECORDING_MAX_REQUEST_BYTES=int(
            os.getenv("RECORDING_MAX_REQUEST_BYTES", str(1024 * 1024 + 1024))
        ),
        RECORDING_SESSION_TTL_SECONDS=int(
            os.getenv("RECORDING_SESSION_TTL_SECONDS", "3600")
        ),
        MAIN_APP_URL=os.getenv(
            "MAIN_APP_URL", "http://127.0.0.1:5000/dashboard"
        ),
    )
    if test_config:
        app.config.update(test_config)

    app.config["MAX_CONTENT_LENGTH"] = app.config["RECORDING_MAX_REQUEST_BYTES"]
    app.extensions["recording_store"] = RecordingStore(
        app.config["RECORDING_STORAGE_ROOT"],
        max_duration_seconds=app.config["RECORDING_MAX_DURATION_SECONDS"],
        max_chunk_bytes=app.config["RECORDING_MAX_CHUNK_BYTES"],
        session_ttl_seconds=app.config["RECORDING_SESSION_TTL_SECONDS"],
    )

    @app.after_request
    def disable_sensitive_caching(response: Response) -> Response:
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "private, no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(RecordingError)
    def handle_recording_error(error: RecordingError):
        return error_response(error.code, str(error), error.status_code)

    @app.errorhandler(413)
    def handle_request_too_large(_error):
        return error_response("request_too_large", "The upload is too large.", 413)

    @app.get("/")
    def recorder_page():
        return render_template(
            "recorder.html",
            main_app_url=app.config["MAIN_APP_URL"],
        )

    @app.get("/dashboard")
    def dashboard_redirect():
        return redirect(app.config["MAIN_APP_URL"])

    @app.get("/healthz")
    def healthcheck():
        return jsonify({"status": "ok"})

    @app.get("/api/v1/config")
    def api_config():
        return jsonify(
            {
                "max_duration_seconds": app.config["RECORDING_MAX_DURATION_SECONDS"],
                "max_chunk_bytes": app.config["RECORDING_MAX_CHUNK_BYTES"],
                "supported_sample_rates": [16000, 48000],
                "format": {
                    "container": "wav",
                    "encoding": "pcm_s16le",
                    "channels": 1,
                    "bit_depth": 16,
                },
            }
        )

    @app.get("/api/v1/prompts")
    def api_prompts():
        prompt_path = Path(app.config["RECORDING_PROMPTS_FILE"])
        if not prompt_path.is_file():
            return jsonify({"prompts": []})
        prompts = [
            line.strip()
            for line in prompt_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return jsonify({"prompts": prompts[:1_000]})

    @app.post("/api/v1/sessions")
    def create_recording_session():
        payload = request.get_json(silent=True) or {}
        try:
            sample_rate = int(payload.get("sample_rate", 16_000))
        except (TypeError, ValueError):
            return error_response("invalid_sample_rate", "Sample rate must be an integer.", 400)

        overwrite_recording_id = payload.get("overwrite_recording_id")
        if overwrite_recording_id is not None:
            overwrite_recording_id = str(overwrite_recording_id).strip() or None

        store = get_store()
        store.cleanup_expired_sessions()
        session = store.create_session(
            user_id=LOCAL_USER_ID,
            prompt=str(payload.get("prompt", "")),
            sample_rate=sample_rate,
            bit_depth=16,
            channels=1,
            overwrite_recording_id=overwrite_recording_id,
        )
        return jsonify(public_session(session)), 201

    @app.put("/api/v1/sessions/<session_id>/chunks/<int:sequence>")
    def upload_recording_chunk(session_id: str, sequence: int):
        if request.mimetype != "application/octet-stream":
            return error_response(
                "unsupported_media_type", "Audio chunks must be binary PCM.", 415
            )
        checksum = request.headers.get("X-Chunk-SHA256")
        result = get_store().append_chunk(
            session_id,
            user_id=LOCAL_USER_ID,
            sequence=sequence,
            source=request.stream,
            expected_sha256=checksum,
        )
        return jsonify(result)

    @app.post("/api/v1/sessions/<session_id>/complete")
    def complete_recording(session_id: str):
        payload = request.get_json(silent=True) or {}
        overwrite_recording_id = payload.get("overwrite_recording_id")
        if overwrite_recording_id is not None:
            overwrite_recording_id = str(overwrite_recording_id).strip() or None
        meta = get_store().finalize(
            session_id,
            user_id=LOCAL_USER_ID,
            overwrite_recording_id=overwrite_recording_id,
        )
        return jsonify(public_recording(meta))

    @app.delete("/api/v1/sessions/<session_id>")
    def abort_recording(session_id: str):
        get_store().abort_session(session_id, user_id=LOCAL_USER_ID)
        return "", 204

    @app.get("/api/v1/recordings")
    def list_recordings():
        try:
            limit = min(max(int(request.args.get("limit", "20")), 1), 100)
        except ValueError:
            return error_response("invalid_limit", "Limit must be an integer.", 400)
        recordings = get_store().list_recordings(
            user_id=LOCAL_USER_ID, limit=limit
        )
        return jsonify({"recordings": [public_recording(item) for item in recordings]})

    @app.get("/api/v1/recordings/<recording_id>")
    def get_recording_metadata(recording_id: str):
        meta, _ = get_store().get_recording(
            recording_id, user_id=LOCAL_USER_ID
        )
        return jsonify(public_recording(meta))

    @app.get("/api/v1/recordings/<recording_id>/audio")
    def get_recording_audio(recording_id: str):
        _, audio_path = get_store().get_recording(
            recording_id, user_id=LOCAL_USER_ID
        )
        response = send_file(audio_path, mimetype="audio/wav", conditional=True)
        response.headers["Cache-Control"] = "private, no-store, max-age=0"
        return response

    @app.delete("/api/v1/recordings/<recording_id>")
    def delete_recording(recording_id: str):
        get_store().delete_recording(recording_id, user_id=LOCAL_USER_ID)
        return "", 204

    @app.cli.command("cleanup-recordings")
    def cleanup_recordings_command():
        """Delete expired, incomplete recording sessions."""
        removed = get_store().cleanup_expired_sessions()
        click.echo(f"Removed {removed} expired recording session(s).")

    return app


def get_store() -> RecordingStore:
    from flask import current_app

    return current_app.extensions["recording_store"]


def public_session(meta: dict) -> dict:
    data = {
        key: meta[key]
        for key in (
            "session_id",
            "status",
            "next_sequence",
            "bytes_received",
            "expires_at",
            "sample_rate",
            "bit_depth",
            "channels",
        )
    }
    if meta.get("overwrite_recording_id"):
        data["overwrite_recording_id"] = meta["overwrite_recording_id"]
    return data


def public_recording(meta: dict) -> dict:
    result = {
        key: meta.get(key)
        for key in (
            "recording_id",
            "status",
            "prompt",
            "duration_seconds",
            "sample_rate",
            "bit_depth",
            "channels",
            "bytes_received",
            "sha256",
            "completed_at",
        )
    }
    if meta.get("recording_id"):
        result["audio_url"] = f"/api/v1/recordings/{meta['recording_id']}/audio"
    return result


def error_response(code: str, message: str, status: int):
    return jsonify({"error": {"code": code, "message": message}}), status


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=False)
