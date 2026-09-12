# Recording Service Planning Session

## Specialist Discussion

### Senior Python / Flask Developer

The desktop draft cannot be embedded in Flask: `sounddevice` captures the server microphone, and its unbounded list of NumPy buffers plus whole-recording concatenation can exhaust memory. The replacement must use a separate Flask service, explicit state transitions, server-generated UUIDs, disk-backed chunks, atomic finalization, strict ownership, and deterministic cleanup.

### Senior Fullstack Developer

The user microphone belongs in the browser through `getUserMedia`. The interface needs visible permission, recording, stopping, review, upload, saved, and error states. A bounded sequential queue prevents memory growth when the network is slow; device tracks, `AudioContext`, timers, analyser animation, queued chunks, and object URLs must all be released.

### Senior AI Engineer

Qwen is not required for reliable capture. Transcription and prompt-match quality checks should be optional asynchronous jobs after durable storage. They must never execute inside upload or finalize requests, and raw audio must not be used as a Redis/process cache value.

## Agreed Scope

1. Add `services/recording_app` as a standalone Flask microservice.
2. Keep local recorder access independent from annotation authentication.
3. Stream mono 16-bit PCM chunks from an `AudioWorklet` to disk.
4. Enforce sequence, checksum, size, duration, ownership, and TTL rules.
5. Atomically finalize WAV plus JSON and TSV metadata.
6. Link the recorder from the existing annotation navigation.
7. Add API, storage, concurrency, ownership, and cleanup tests.
8. Defer Qwen transcription and semantic quality checks to a future queue-backed worker.

## Cache and Lifecycle Policy

- Browser memory holds only a bounded upload queue and a small analyser buffer.
- Flask streams request bodies in 64 KiB blocks and never assembles full audio in RAM.
- Temporary chunks expire; final audio is durable content rather than cache.
- File locks serialize chunk metadata and finalization across threads/processes.
- Atomic rename prevents partial WAV or JSON files from appearing complete.
- Cleanup runs externally or opportunistically, never in a per-worker background thread.
