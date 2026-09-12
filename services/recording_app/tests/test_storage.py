from __future__ import annotations

import hashlib
import io
import json
import wave
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from services.recording_app.storage import (
    RecordingConflict,
    RecordingForbidden,
    RecordingNotFound,
    RecordingStore,
)


def pcm_bytes(frame_count: int = 3_200) -> bytes:
    return b"\x10\x00" * frame_count


@pytest.fixture
def store(tmp_path):
    return RecordingStore(
        tmp_path,
        max_duration_seconds=2,
        max_chunk_bytes=32_000,
        session_ttl_seconds=60,
    )


def test_chunk_streaming_and_atomic_wav_finalize(store):
    session = store.create_session(
        user_id="7", prompt="နေကောင်းလား", sample_rate=16_000
    )
    audio = pcm_bytes()
    result = store.append_chunk(
        session["session_id"],
        user_id="7",
        sequence=0,
        source=io.BytesIO(audio),
        expected_sha256=hashlib.sha256(audio).hexdigest(),
    )

    assert result["received_bytes"] == len(audio)
    completed = store.finalize(session["session_id"], user_id="7")
    assert completed["status"] == "ready"
    assert completed["duration_seconds"] == 0.2

    metadata, audio_path = store.get_recording(completed["recording_id"], user_id="7")
    assert metadata["prompt"] == "နေကောင်းလား"
    with wave.open(str(audio_path), "rb") as wav_file:
        assert wav_file.getnchannels() == 1
        assert wav_file.getsampwidth() == 2
        assert wav_file.getframerate() == 16_000
        assert wav_file.getnframes() == 3_200


def test_sequence_and_owner_are_enforced(store):
    session = store.create_session(user_id="7", prompt="test", sample_rate=16_000)

    with pytest.raises(RecordingConflict):
        store.append_chunk(
            session["session_id"], user_id="7", sequence=1, source=io.BytesIO(pcm_bytes())
        )
    with pytest.raises(RecordingForbidden):
        store.append_chunk(
            session["session_id"], user_id="8", sequence=0, source=io.BytesIO(pcm_bytes())
        )


def test_finalize_is_idempotent_under_concurrency(store):
    session = store.create_session(user_id="7", prompt="test", sample_rate=16_000)
    store.append_chunk(
        session["session_id"], user_id="7", sequence=0, source=io.BytesIO(pcm_bytes())
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: store.finalize(session["session_id"], user_id="7"), range(2)
            )
        )

    assert results[0]["recording_id"] == results[1]["recording_id"]
    manifest_lines = store.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(manifest_lines) == 2


def test_expired_session_cleanup(store):
    session = store.create_session(user_id="7", prompt="test", sample_rate=16_000)
    metadata_path = store.sessions_root / session["session_id"] / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    assert store.cleanup_expired_sessions() == 1
    assert not metadata_path.exists()


def test_append_reads_in_bounded_blocks(store):
    class GuardedStream(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 65_536
            return super().read(size)

    session = store.create_session(user_id="7", prompt="test", sample_rate=16_000)
    store.append_chunk(
        session["session_id"],
        user_id="7",
        sequence=0,
        source=GuardedStream(pcm_bytes()),
    )


def test_retry_nonexistent_recording_fails(store):
    with pytest.raises(RecordingNotFound):
        store.create_session(
            user_id="7",
            prompt="test",
            sample_rate=16_000,
            overwrite_recording_id="00000000-0000-0000-0000-000000000000",
        )


def test_retry_and_overwrite_recording_updates_file_and_manifest(store):
    session1 = store.create_session(
        user_id="7", prompt="ပထမအသံ", sample_rate=16_000
    )
    store.append_chunk(
        session1["session_id"],
        user_id="7",
        sequence=0,
        source=io.BytesIO(pcm_bytes(3_200)),
    )
    recording1 = store.finalize(session1["session_id"], user_id="7")
    rec_id = recording1["recording_id"]
    assert recording1["duration_seconds"] == 0.2

    meta1, audio_path1 = store.get_recording(rec_id, user_id="7")
    assert meta1["prompt"] == "ပထမအသံ"

    manifest_lines = store.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(manifest_lines) == 2  # header + 1 record

    # Retry with different audio length and updated prompt
    session2 = store.create_session(
        user_id="7",
        prompt="ဒုတိယအသံ (ပြန်ဆိုထားသည်)",
        sample_rate=16_000,
        overwrite_recording_id=rec_id,
    )
    # 6400 frames = 0.4 seconds
    audio2 = b"\x20\x00" * 6_400
    store.append_chunk(
        session2["session_id"],
        user_id="7",
        sequence=0,
        source=io.BytesIO(audio2),
        expected_sha256=hashlib.sha256(audio2).hexdigest(),
    )
    overwritten = store.finalize(session2["session_id"], user_id="7")
    assert overwritten["recording_id"] == rec_id
    assert overwritten["duration_seconds"] == 0.4
    assert overwritten["prompt"] == "ဒုတိယအသံ (ပြန်ဆိုထားသည်)"
    assert overwritten["sha256"] == hashlib.sha256(audio_path1.read_bytes()).hexdigest()

    # Verify existing file was overwritten in-place
    meta2, audio_path2 = store.get_recording(rec_id, user_id="7")
    assert audio_path2 == audio_path1
    assert meta2["duration_seconds"] == 0.4
    assert meta2["prompt"] == "ဒုတိယအသံ (ပြန်ဆိုထားသည်)"
    with wave.open(str(audio_path2), "rb") as wav_file:
        assert wav_file.getnframes() == 6_400

    # Verify manifest still only has 1 record (plus header) and updated content
    manifest_lines2 = store.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(manifest_lines2) == 2
    assert "ဒုတိယအသံ (ပြန်ဆိုထားသည်)" in manifest_lines2[1]
    assert "0.4" in manifest_lines2[1]
