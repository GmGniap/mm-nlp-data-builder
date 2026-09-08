from __future__ import annotations

import hashlib
import io
import wave

import pytest

from services.recording_app.app import create_app
from shared.recording_auth import create_recording_token


SECRET = "test-recording-secret"


@pytest.fixture
def app(tmp_path):
    return create_app(
        {
            "TESTING": True,
            "RECORDING_TOKEN_SECRET": SECRET,
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


def headers(user_id="7"):
    token = create_recording_token(SECRET, user_id, f"user{user_id}@example.com")
    return {"Authorization": f"Bearer {token}"}


def create_session(client, user_id="7"):
    response = client.post(
        "/api/v1/sessions",
        headers=headers(user_id),
        json={"prompt": "စမ်းသပ် အသံ", "sample_rate": 16000},
    )
    assert response.status_code == 201
    return response.get_json()


def test_api_requires_signed_token(client):
    response = client.get("/api/v1/config")
    assert response.status_code == 401
    assert response.get_json()["error"]["code"] == "authentication_required"


def test_recording_api_round_trip_and_no_store_headers(client):
    session = create_session(client)
    audio = b"\x20\x00" * 3_200
    digest = hashlib.sha256(audio).hexdigest()
    chunk_response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/0",
        headers={
            **headers(),
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": digest,
        },
        data=audio,
    )
    assert chunk_response.status_code == 200
    assert chunk_response.headers["Cache-Control"].startswith("private, no-store")

    complete = client.post(
        f"/api/v1/sessions/{session['session_id']}/complete", headers=headers(), json={}
    )
    assert complete.status_code == 200
    recording = complete.get_json()
    assert recording["status"] == "ready"
    assert recording["duration_seconds"] == 0.2

    audio_response = client.get(recording["audio_url"], headers=headers())
    assert audio_response.status_code == 200
    assert audio_response.mimetype == "audio/wav"
    with wave.open(io.BytesIO(audio_response.data), "rb") as wav_file:
        assert wav_file.getframerate() == 16_000
        assert wav_file.getnframes() == 3_200


def test_out_of_order_chunk_returns_conflict(client):
    session = create_session(client)
    response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/1",
        headers={**headers(), "Content-Type": "application/octet-stream"},
        data=b"\x00\x00" * 200,
    )
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "invalid_state"


def test_other_user_cannot_access_session(client):
    session = create_session(client, user_id="7")
    response = client.delete(
        f"/api/v1/sessions/{session['session_id']}", headers=headers("8")
    )
    assert response.status_code == 403


def test_unsupported_chunk_type_is_rejected(client):
    session = create_session(client)
    response = client.put(
        f"/api/v1/sessions/{session['session_id']}/chunks/0",
        headers={**headers(), "Content-Type": "audio/webm"},
        data=b"not pcm",
    )
    assert response.status_code == 415
