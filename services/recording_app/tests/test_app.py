from __future__ import annotations

import hashlib
import io
import re
import wave
from datetime import datetime

import pytest

from services.recording_app.app import create_app


@pytest.fixture
def app(tmp_path):
    return create_app(
        {
            "TESTING": True,
            "RECORDING_STORAGE_ROOT": str(tmp_path),
            "RECORDING_PROMPTS_FILE": str(tmp_path / "prompts.txt"),
            "RECORDING_MAX_DURATION_SECONDS": 2,
            "RECORDING_MAX_CHUNK_BYTES": 32_000,
            "RECORDING_MAX_REQUEST_BYTES": 33_024,
        }
    )


@pytest.fixture
def client(app):
    return app.test_client()


def create_session(client):
    response = client.post(
        "/api/v1/sessions",
        json={"prompt": "စမ်းသပ် အသံ", "sample_rate": 16000},
    )
    assert response.status_code == 201
    return response.get_json()


def test_api_is_available_without_token(client):
    response = client.get("/api/v1/config")
    assert response.status_code == 200


def test_recording_api_round_trip_and_no_store_headers(client):
    session = create_session(client)
    audio = b"\x20\x00" * 3_200
    digest = hashlib.sha256(audio).hexdigest()
    chunk_response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/0",
        headers={
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": digest,
        },
        data=audio,
    )
    assert chunk_response.status_code == 200
    assert chunk_response.headers["Cache-Control"].startswith("private, no-store")

    complete = client.post(
        f"/api/v1/sessions/{session['session_id']}/complete", json={}
    )
    assert complete.status_code == 200
    recording = complete.get_json()
    assert recording["status"] == "ready"
    assert recording["duration_seconds"] == 0.2

    audio_response = client.get(recording["audio_url"])
    assert audio_response.status_code == 200
    assert audio_response.mimetype == "audio/wav"
    with wave.open(io.BytesIO(audio_response.data), "rb") as wav_file:
        assert wav_file.getframerate() == 16_000
        assert wav_file.getnframes() == 3_200


def test_out_of_order_chunk_returns_conflict(client):
    session = create_session(client)
    response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/1",
        headers={"Content-Type": "application/octet-stream"},
        data=b"\x00\x00" * 200,
    )
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "invalid_state"


def test_standalone_page_loads_without_authentication(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"Speech Recorder" in response.data


def test_unsupported_chunk_type_is_rejected(client):
    session = create_session(client)
    response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/0",
        headers={"Content-Type": "audio/webm"},
        data=b"not pcm",
    )
    assert response.status_code == 415


def test_standalone_page_has_retry_button(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b'id="retryButton"' in response.data
    assert b"Retry" in response.data


def test_standalone_page_has_file_naming_dropdown_next_to_order(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b'id="namingMode"' in response.data
    assert b"Default (Hash)" in response.data
    assert b"Formatted" in response.data
    assert b'id="annotatorInput"' in response.data
    body = response.data.decode("utf-8")
    assert body.find('id="promptMode"') < body.find('id="namingMode"') < body.find('id="jumpPromptInput"')


def test_standalone_page_has_jump_to_number_elements(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"Jump to number" in response.data
    assert b'id="jumpPromptInput"' in response.data
    assert b'id="jumpPromptButton"' in response.data
    # Verify jump elements are located in prompt-actions next to promptMode
    body = response.data.decode("utf-8")
    assert body.find('id="promptMode"') < body.find('id="jumpPromptInput"') < body.find('id="nextPromptButton"')


def test_standalone_page_has_back_to_main_button(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b'id="backToMainButton"' in response.data
    assert b"Back to main" in response.data
    body = response.data.decode("utf-8")
    assert 'href="http://127.0.0.1:5000/dashboard"' in body
    # Verify back button and serviceBadge are co-located in hero-actions
    assert '<div class="hero-actions">' in body
    assert body.find('id="backToMainButton"') < body.find('id="serviceBadge"')


def test_dashboard_redirect_route(client):
    response = client.get("/dashboard")
    assert response.status_code == 302
    assert response.headers["Location"] == "http://127.0.0.1:5000/dashboard"


def test_custom_main_app_url(tmp_path):
    custom_url = "http://custom-host:8080/my-dashboard"
    app = create_app(
        {
            "RECORDING_STORAGE_ROOT": str(tmp_path / "data"),
            "RECORDING_PROMPTS_FILE": str(tmp_path / "prompts.txt"),
            "MAIN_APP_URL": custom_url,
        }
    )
    with app.test_client() as custom_client:
        response = custom_client.get("/")
        assert response.status_code == 200
        assert f'href="{custom_url}"'.encode("utf-8") in response.data

        redirect_resp = custom_client.get("/dashboard")
        assert redirect_resp.status_code == 302
        assert redirect_resp.headers["Location"] == custom_url


def test_api_retry_and_overwrite_round_trip(client):
    session1 = create_session(client)
    audio1 = b"\x10\x00" * 3_200
    client.put(
        f"/api/v1/sessions/{session1['session_id']}/chunks/0",
        headers={
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(audio1).hexdigest(),
        },
        data=audio1,
    )
    complete1 = client.post(
        f"/api/v1/sessions/{session1['session_id']}/complete", json={}
    )
    recording1 = complete1.get_json()
    rec_id = recording1["recording_id"]
    assert recording1["duration_seconds"] == 0.2

    # Start retry session with overwrite_recording_id
    retry_session_resp = client.post(
        "/api/v1/sessions",
        json={
            "prompt": "စမ်းသပ် ပြန်ဆိုသံ",
            "sample_rate": 16000,
            "overwrite_recording_id": rec_id,
        },
    )
    assert retry_session_resp.status_code == 201
    retry_session = retry_session_resp.get_json()
    assert retry_session["overwrite_recording_id"] == rec_id

    # Upload chunk with 6400 frames (0.4s)
    audio2 = b"\x25\x00" * 6_400
    client.put(
        f"/api/v1/sessions/{retry_session['session_id']}/chunks/0",
        headers={
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(audio2).hexdigest(),
        },
        data=audio2,
    )
    complete2 = client.post(
        f"/api/v1/sessions/{retry_session['session_id']}/complete", json={}
    )
    assert complete2.status_code == 200
    recording2 = complete2.get_json()
    assert recording2["recording_id"] == rec_id
    assert recording2["prompt"] == "စမ်းသပ် ပြန်ဆိုသံ"
    assert recording2["duration_seconds"] == 0.4
    assert recording2["sha256"] != recording1["sha256"]

    # Verify audio endpoint delivers the new audio
    audio_resp = client.get(recording2["audio_url"])
    assert audio_resp.status_code == 200
    with wave.open(io.BytesIO(audio_resp.data), "rb") as wav_file:
        assert wav_file.getnframes() == 6_400


def test_api_formatted_file_naming_round_trip(client):
    response = client.post(
        "/api/v1/sessions",
        json={
            "prompt": "စမ်းသပ် စာသား",
            "sample_rate": 16000,
            "naming_mode": "formatted",
            "annotator_username": "mgmg",
            "prompt_order_number": 5,
        },
    )
    assert response.status_code == 201
    session = response.get_json()
    assert session["naming_mode"] == "formatted"
    assert session["annotator_username"] == "mgmg"
    assert session["prompt_order_number"] == 5

    audio = b"\x10\x00" * 3_200
    client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/0",
        headers={
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(audio).hexdigest(),
        },
        data=audio,
    )

    complete = client.post(
        f"/api/v1/sessions/{session['session_id']}/complete",
        json={
            "naming_mode": "formatted",
            "annotator_username": "mgmg",
            "prompt_order_number": 5,
        },
    )
    assert complete.status_code == 200
    recording = complete.get_json()

    # Format: <annotator_username_short>_<date>_<timestamp>_<prompt_order_number>
    # Date: yyyymmdd. Timestamp: hr+minute+second in 2 digits. Derived from completed_at.
    rec_id = recording["recording_id"]
    completed_at = recording["completed_at"]
    completed_dt = datetime.fromisoformat(completed_at)
    expected_date = completed_dt.strftime("%Y%m%d")
    expected_time = completed_dt.strftime("%H%M%S")
    expected_id = f"mgmg_{expected_date}_{expected_time}_5"

    assert rec_id == expected_id
    assert recording["audio_url"] == f"/api/v1/recordings/{expected_id}/audio"

    audio_response = client.get(recording["audio_url"])
    assert audio_response.status_code == 200
    assert audio_response.mimetype == "audio/wav"
    with wave.open(io.BytesIO(audio_response.data), "rb") as wav_file:
        assert wav_file.getframerate() == 16_000
        assert wav_file.getnframes() == 3_200


def test_storage_path_resolution_independent_of_cwd(monkeypatch, tmp_path):
    from services.recording_app.app import BASE_DIR, PROJECT_ROOT, resolve_app_path

    # Verify resolve_app_path with relative services path
    resolved_root = resolve_app_path("services/recording_app/data")
    assert resolved_root == PROJECT_ROOT / "services/recording_app/data"
    assert resolved_root == BASE_DIR / "data"

    # Verify when CWD is inside services/recording_app directory
    monkeypatch.chdir(BASE_DIR)
    app = create_app({"RECORDING_STORAGE_ROOT": "services/recording_app/data"})
    assert app.config["RECORDING_STORAGE_ROOT"] == str(BASE_DIR / "data")
    assert app.extensions["recording_store"].root == BASE_DIR / "data"
