"""Disk-backed recording session storage with bounded, atomic writes."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import shutil
import uuid
import wave
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, Iterator


class RecordingError(Exception):
    """Base error raised by the recording store."""

    status_code = 400
    code = "recording_error"


class RecordingNotFound(RecordingError):
    status_code = 404
    code = "not_found"


class RecordingForbidden(RecordingError):
    status_code = 403
    code = "forbidden"


class RecordingConflict(RecordingError):
    status_code = 409
    code = "invalid_state"


class RecordingTooLarge(RecordingError):
    status_code = 413
    code = "recording_too_large"


class RecordingInvalid(RecordingError):
    status_code = 422
    code = "invalid_recording"


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


class RecordingStore:
    """Stores upload chunks outside process memory and finalizes PCM as WAV."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_duration_seconds: int = 120,
        max_chunk_bytes: int = 1_048_576,
        session_ttl_seconds: int = 3_600,
    ) -> None:
        self.root = Path(root).resolve()
        self.sessions_root = self.root / "sessions"
        self.recordings_root = self.root / "recordings"
        self.manifest_path = self.root / "recordings.tsv"
        self.max_duration_seconds = max_duration_seconds
        self.max_chunk_bytes = max_chunk_bytes
        self.session_ttl_seconds = session_ttl_seconds
        self.sessions_root.mkdir(parents=True, exist_ok=True)
        self.recordings_root.mkdir(parents=True, exist_ok=True)

    def create_session(
        self,
        *,
        user_id: str,
        prompt: str,
        sample_rate: int,
        bit_depth: int = 16,
        channels: int = 1,
    ) -> dict:
        prompt = prompt.strip()
        if not prompt:
            raise RecordingInvalid("A non-empty prompt is required.")
        if len(prompt) > 2_000:
            raise RecordingInvalid("The prompt is too long.")
        if sample_rate not in {8_000, 16_000, 22_050, 44_100, 48_000}:
            raise RecordingInvalid("Unsupported sample rate.")
        if bit_depth != 16 or channels != 1:
            raise RecordingInvalid("Only mono 16-bit PCM is supported.")

        session_id = str(uuid.uuid4())
        session_dir = self.sessions_root / session_id
        (session_dir / "chunks").mkdir(parents=True)
        now = utc_now()
        meta = {
            "session_id": session_id,
            "user_id": str(user_id),
            "prompt": prompt,
            "sample_rate": sample_rate,
            "bit_depth": bit_depth,
            "channels": channels,
            "status": "recording",
            "next_sequence": 0,
            "bytes_received": 0,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=self.session_ttl_seconds)).isoformat(),
        }
        self._write_json_atomic(session_dir / "metadata.json", meta)
        return meta

    def append_chunk(
        self,
        session_id: str,
        *,
        user_id: str,
        sequence: int,
        source: BinaryIO,
        expected_sha256: str | None = None,
    ) -> dict:
        session_dir = self._session_dir(session_id)
        with self._locked(session_dir / ".lock"):
            meta = self._load_owned_session(session_dir, user_id)
            if meta["status"] != "recording":
                raise RecordingConflict("This recording no longer accepts chunks.")
            if sequence != meta["next_sequence"]:
                raise RecordingConflict(
                    f"Expected chunk {meta['next_sequence']}, received {sequence}."
                )

            chunk_path = session_dir / "chunks" / f"{sequence:08d}.pcm"
            part_path = chunk_path.with_suffix(".part")
            digest = hashlib.sha256()
            written = 0
            try:
                with part_path.open("wb") as target:
                    while True:
                        block = source.read(65_536)
                        if not block:
                            break
                        written += len(block)
                        if written > self.max_chunk_bytes:
                            raise RecordingTooLarge("The audio chunk exceeds the size limit.")
                        digest.update(block)
                        target.write(block)
                    target.flush()
                    os.fsync(target.fileno())
            except Exception:
                part_path.unlink(missing_ok=True)
                raise

            if written == 0 or written % 2:
                part_path.unlink(missing_ok=True)
                raise RecordingInvalid("The PCM chunk is empty or truncated.")

            actual_sha256 = digest.hexdigest()
            if expected_sha256 and expected_sha256.lower() != actual_sha256:
                part_path.unlink(missing_ok=True)
                raise RecordingInvalid("The audio chunk checksum does not match.")

            max_bytes = self._max_pcm_bytes(meta)
            if meta["bytes_received"] + written > max_bytes:
                part_path.unlink(missing_ok=True)
                raise RecordingTooLarge("The recording exceeds the duration limit.")

            os.replace(part_path, chunk_path)
            meta["next_sequence"] += 1
            meta["bytes_received"] += written
            meta["updated_at"] = iso_now()
            self._write_json_atomic(session_dir / "metadata.json", meta)
            return {
                "accepted_sequence": sequence,
                "next_sequence": meta["next_sequence"],
                "chunk_bytes": written,
                "received_bytes": meta["bytes_received"],
                "sha256": actual_sha256,
            }

    def finalize(self, session_id: str, *, user_id: str) -> dict:
        session_dir = self._session_dir(session_id)
        with self._locked(session_dir / ".lock"):
            meta = self._load_owned_session(session_dir, user_id)
            if meta["status"] == "ready":
                return meta
            if meta["status"] != "recording":
                raise RecordingConflict("This recording cannot be finalized.")
            if meta["bytes_received"] == 0:
                raise RecordingInvalid("No audio was uploaded.")

            chunk_paths = sorted((session_dir / "chunks").glob("*.pcm"))
            if len(chunk_paths) != meta["next_sequence"]:
                raise RecordingConflict("One or more audio chunks are missing.")

            recording_id = str(uuid.uuid4())
            day_dir = self.recordings_root / utc_now().strftime("%Y-%m-%d")
            day_dir.mkdir(parents=True, exist_ok=True)
            final_path = day_dir / f"{recording_id}.wav"
            part_path = final_path.with_suffix(".wav.part")

            try:
                with wave.open(str(part_path), "wb") as wav_file:
                    wav_file.setnchannels(meta["channels"])
                    wav_file.setsampwidth(meta["bit_depth"] // 8)
                    wav_file.setframerate(meta["sample_rate"])
                    for chunk_path in chunk_paths:
                        with chunk_path.open("rb") as chunk_file:
                            for block in iter(lambda: chunk_file.read(65_536), b""):
                                wav_file.writeframesraw(block)
                os.replace(part_path, final_path)
            except Exception:
                part_path.unlink(missing_ok=True)
                raise

            duration_seconds = meta["bytes_received"] / (
                meta["sample_rate"] * meta["channels"] * (meta["bit_depth"] // 8)
            )
            if duration_seconds < 0.1:
                final_path.unlink(missing_ok=True)
                raise RecordingInvalid("The recording is too short.")

            meta.update(
                {
                    "recording_id": recording_id,
                    "status": "ready",
                    "duration_seconds": round(duration_seconds, 3),
                    "sha256": self._sha256_file(final_path),
                    "storage_path": str(final_path.relative_to(self.root)),
                    "completed_at": iso_now(),
                    "updated_at": iso_now(),
                }
            )
            self._write_json_atomic(final_path.with_suffix(".json"), meta)
            self._write_json_atomic(session_dir / "metadata.json", meta)
            self._append_manifest(meta)
            shutil.rmtree(session_dir / "chunks", ignore_errors=True)
            return meta

    def get_recording(self, recording_id: str, *, user_id: str) -> tuple[dict, Path]:
        for metadata_path in self.recordings_root.glob(f"*/{recording_id}.json"):
            meta = self._read_json(metadata_path)
            self._assert_owner(meta, user_id)
            audio_path = metadata_path.with_suffix(".wav")
            if not audio_path.is_file():
                raise RecordingNotFound("The audio file is unavailable.")
            return meta, audio_path
        raise RecordingNotFound("Recording not found.")

    def list_recordings(self, *, user_id: str, limit: int = 50) -> list[dict]:
        results: list[dict] = []
        paths = sorted(
            self.recordings_root.glob("*/*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in paths:
            meta = self._read_json(path)
            if str(meta.get("user_id")) == str(user_id):
                results.append(meta)
                if len(results) >= limit:
                    break
        return results

    def abort_session(self, session_id: str, *, user_id: str) -> None:
        session_dir = self._session_dir(session_id)
        with self._locked(session_dir / ".lock"):
            meta = self._load_owned_session(session_dir, user_id)
            if meta["status"] == "ready":
                raise RecordingConflict("A completed recording cannot be aborted.")
        shutil.rmtree(session_dir, ignore_errors=True)

    def delete_recording(self, recording_id: str, *, user_id: str) -> None:
        _, audio_path = self.get_recording(recording_id, user_id=user_id)
        with self._locked(audio_path.with_suffix(".lock")):
            audio_path.unlink(missing_ok=True)
            audio_path.with_suffix(".json").unlink(missing_ok=True)
            audio_path.with_suffix(".lock").unlink(missing_ok=True)

    def cleanup_expired_sessions(self, *, now: datetime | None = None) -> int:
        now = now or utc_now()
        removed = 0
        for session_dir in self.sessions_root.iterdir():
            if not session_dir.is_dir():
                continue
            try:
                meta = self._read_json(session_dir / "metadata.json")
                expires_at = datetime.fromisoformat(meta["expires_at"])
            except (OSError, KeyError, ValueError, json.JSONDecodeError):
                expires_at = datetime.fromtimestamp(session_dir.stat().st_mtime, UTC) + timedelta(
                    seconds=self.session_ttl_seconds
                )
            if expires_at <= now:
                shutil.rmtree(session_dir, ignore_errors=True)
                removed += 1
        return removed

    def _session_dir(self, session_id: str) -> Path:
        try:
            normalized = str(uuid.UUID(session_id))
        except ValueError as exc:
            raise RecordingNotFound("Recording session not found.") from exc
        session_dir = self.sessions_root / normalized
        if not session_dir.is_dir():
            raise RecordingNotFound("Recording session not found.")
        return session_dir

    def _load_owned_session(self, session_dir: Path, user_id: str) -> dict:
        try:
            meta = self._read_json(session_dir / "metadata.json")
        except (OSError, json.JSONDecodeError) as exc:
            raise RecordingNotFound("Recording session not found.") from exc
        self._assert_owner(meta, user_id)
        return meta

    @staticmethod
    def _assert_owner(meta: dict, user_id: str) -> None:
        if str(meta.get("user_id")) != str(user_id):
            raise RecordingForbidden("You do not own this recording.")

    def _max_pcm_bytes(self, meta: dict) -> int:
        return (
            self.max_duration_seconds
            * meta["sample_rate"]
            * meta["channels"]
            * (meta["bit_depth"] // 8)
        )

    @staticmethod
    def _read_json(path: Path) -> dict:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def _write_json_atomic(path: Path, value: dict) -> None:
        temp_path = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(value, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for block in iter(lambda: file.read(65_536), b""):
                digest.update(block)
        return digest.hexdigest()

    def _append_manifest(self, meta: dict) -> None:
        fields = [
            "recording_id",
            "prompt",
            "completed_at",
            "duration_seconds",
            "sample_rate",
            "bit_depth",
            "sha256",
            "storage_path",
            "user_id",
        ]
        lock_path = self.manifest_path.with_suffix(".lock")
        with self._locked(lock_path):
            exists = self.manifest_path.exists() and self.manifest_path.stat().st_size > 0
            with self.manifest_path.open("a", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fields, delimiter="\t")
                if not exists:
                    writer.writeheader()
                writer.writerow({field: meta.get(field, "") for field in fields})
                file.flush()
                os.fsync(file.fileno())

    @staticmethod
    @contextmanager
    def _locked(path: Path) -> Iterator[None]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
