# Recording Service

Browser-based prompted speech capture for the MM NLP Data Builder. The browser captures mono PCM in an `AudioWorklet`, sends bounded sequential chunks, and the Flask service writes each chunk directly to disk. Finalization atomically creates a WAV file and JSON/TSV metadata without retaining the full recording in process memory.

## Architecture

- The service runs independently and does not require annotation-app authentication.
- The annotation app's `/record` route simply redirects to the configured recorder URL.
- PCM chunks are limited, checksummed, sequenced, and stored under an expiring session directory.
- Final WAV files use UUID names and are committed with an atomic rename.
- Raw audio is never stored in Flask sessions, Redis, PostgreSQL, or process globals.
- Expired incomplete sessions are removed opportunistically on session creation or with the cleanup CLI command.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `RECORDING_STORAGE_ROOT` | `services/recording_app/data` | Persistent storage or mounted volume. |
| `RECORDING_PROMPTS_FILE` | bundled `prompts.txt` | UTF-8 prompt file, one prompt per line. |
| `RECORDING_MAX_DURATION_SECONDS` | `120` | Hard server-side PCM duration limit. |
| `RECORDING_MAX_CHUNK_BYTES` | `1048576` | Maximum request chunk size. |
| `RECORDING_SESSION_TTL_SECONDS` | `3600` | Lifetime of incomplete sessions. |

The annotation service also accepts `RECORDING_SERVICE_URL`, which points to the public recorder URL. In production, route both services through HTTPS because browser microphone access requires a secure context.

## Run

```bash
uv run --with-requirements services/recording_app/requirements.txt \
  flask --app services.recording_app.app run --port 5001
```

The recording service loads the project-root `.env` file even when it is started directly with `python services/recording_app/app.py`.

## Cleanup

Run this command from cron or the platform scheduler. It is deliberately external rather than a background thread inside each WSGI worker.

```bash
uv run --with-requirements services/recording_app/requirements.txt \
  flask --app services.recording_app.app cleanup-recordings
```

## API

- `GET /api/v1/config`
- `GET /api/v1/prompts`
- `POST /api/v1/sessions`
- `PUT /api/v1/sessions/{session_id}/chunks/{sequence}`
- `POST /api/v1/sessions/{session_id}/complete`
- `DELETE /api/v1/sessions/{session_id}`
- `GET /api/v1/recordings`
- `GET /api/v1/recordings/{recording_id}`
- `GET /api/v1/recordings/{recording_id}/audio`
- `DELETE /api/v1/recordings/{recording_id}`

Audio responses and API metadata use `Cache-Control: private, no-store`.

This simplified service has no user authentication. Bind it to localhost for local use. Add gateway authentication before exposing it to a public network.

## Tests

```bash
uv run --with-requirements services/recording_app/requirements-dev.txt pytest services/recording_app/tests
```
