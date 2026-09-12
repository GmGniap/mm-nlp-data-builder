const elements = {
    backToMainButton: document.querySelector('#backToMainButton'),
    serviceBadge: document.querySelector('#serviceBadge'),
    promptText: document.querySelector('#promptText'),
    promptMode: document.querySelector('#promptMode'),
    jumpPromptInput: document.querySelector('#jumpPromptInput'),
    jumpPromptButton: document.querySelector('#jumpPromptButton'),
    nextPromptButton: document.querySelector('#nextPromptButton'),
    promptCounter: document.querySelector('#promptCounter'),
    sampleRate: document.querySelector('#sampleRate'),
    recordButton: document.querySelector('#recordButton'),
    recordButtonLabel: document.querySelector('#recordButtonLabel'),
    elapsed: document.querySelector('#elapsed'),
    limit: document.querySelector('#limit'),
    meter: document.querySelector('#meter'),
    status: document.querySelector('#status'),
    error: document.querySelector('#error'),
    discardButton: document.querySelector('#discardButton'),
    saveButton: document.querySelector('#saveButton'),
    reviewPanel: document.querySelector('#reviewPanel'),
    playback: document.querySelector('#playback'),
    savedDuration: document.querySelector('#savedDuration'),
    savedRate: document.querySelector('#savedRate'),
    retryButton: document.querySelector('#retryButton'),
};

const state = {
    phase: 'booting',
    prompts: [],
    promptIndex: -1,
    randomOrder: [],
    sessionId: null,
    recordingId: null,
    lastSaved: null,
    pendingOverwriteRecordingId: null,
    overwriteRecordingId: null,
    audioContext: null,
    mediaStream: null,
    sourceNode: null,
    workletNode: null,
    analyser: null,
    animationFrame: null,
    timer: null,
    startedAt: null,
    maxDurationSeconds: 120,
    nextSequence: 0,
    queue: [],
    queuedBytes: 0,
    maxQueuedBytes: 4 * 1024 * 1024,
    uploading: false,
    uploadError: null,
    uploadWaiters: [],
    playbackUrl: null,
    stopping: false,
};

async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (options.body && typeof options.body === 'string') headers.set('Content-Type', 'application/json');
    const response = await fetch(path, { ...options, headers, cache: 'no-store' });
    if (!response.ok) {
        let message = `Request failed (${response.status}).`;
        try {
            const body = await response.json();
            message = body.error?.message || message;
        } catch (_) {}
        throw new Error(message);
    }
    if (response.status === 204) return null;
    return response.json();
}

function setPhase(phase, message) {
    state.phase = phase;
    if (message) elements.status.textContent = message;
    const isRecording = phase === 'recording';
    const busy = ['requesting', 'stopping', 'uploading'].includes(phase);
    elements.recordButton.disabled = busy || ['booting', 'unauthorized'].includes(phase);
    elements.recordButton.setAttribute('aria-pressed', String(isRecording));
    elements.recordButtonLabel.textContent = isRecording ? 'Stop recording' : 'Start recording';
    elements.saveButton.disabled = phase !== 'review';
    elements.discardButton.disabled = !state.sessionId || phase === 'uploading';
    elements.promptText.disabled = isRecording || busy;
    elements.promptMode.disabled = isRecording || busy;
    elements.nextPromptButton.disabled = isRecording || busy;
    if (elements.jumpPromptInput) elements.jumpPromptInput.disabled = isRecording || busy;
    if (elements.jumpPromptButton) elements.jumpPromptButton.disabled = isRecording || busy;
    elements.sampleRate.disabled = isRecording || busy;
}

function showError(message) {
    elements.error.textContent = message;
    elements.error.hidden = false;
}

function clearError() {
    elements.error.hidden = true;
    elements.error.textContent = '';
}

function formatTime(totalSeconds) {
    const seconds = Math.max(0, Math.floor(totalSeconds));
    return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
}

function nextPrompt(clearOverwrite = true) {
    clearError();
    if (clearOverwrite) {
        state.pendingOverwriteRecordingId = null;
        state.overwriteRecordingId = null;
    }
    if (!state.prompts.length) return;
    const mode = elements.promptMode.value;
    if (mode === 'random') {
        if (!state.randomOrder.length) {
            state.randomOrder = state.prompts.map((_, index) => index);
            for (let index = state.randomOrder.length - 1; index > 0; index -= 1) {
                const swap = Math.floor(Math.random() * (index + 1));
                [state.randomOrder[index], state.randomOrder[swap]] = [state.randomOrder[swap], state.randomOrder[index]];
            }
        }
        state.promptIndex = state.randomOrder.shift();
    } else if (mode === 'sequential') {
        state.promptIndex = (state.promptIndex + 1) % state.prompts.length;
    } else {
        state.promptIndex = Math.min(state.promptIndex + 1, state.prompts.length - 1);
    }
    elements.promptText.value = state.prompts[state.promptIndex];
    elements.promptCounter.textContent = `Prompt ${state.promptIndex + 1} of ${state.prompts.length}`;
}

function jumpToPrompt() {
    clearError();
    if (state.phase === 'recording' || ['requesting', 'stopping', 'uploading'].includes(state.phase)) {
        return;
    }
    if (!state.prompts.length) {
        showError('No prompts available to jump to.');
        return;
    }
    const rawValue = elements.jumpPromptInput ? elements.jumpPromptInput.value.trim() : '';
    if (!rawValue) {
        showError('Please enter a prompt number.');
        return;
    }
    const targetNumber = Number(rawValue);
    if (!Number.isInteger(targetNumber) || targetNumber < 1 || targetNumber > state.prompts.length) {
        showError(`Prompt number ${rawValue} is out of range. Please enter a number between 1 and ${state.prompts.length}.`);
        if (elements.jumpPromptInput) {
            elements.jumpPromptInput.focus();
            elements.jumpPromptInput.select();
        }
        return;
    }
    state.pendingOverwriteRecordingId = null;
    state.overwriteRecordingId = null;
    state.promptIndex = targetNumber - 1;
    elements.promptText.value = state.prompts[state.promptIndex];
    elements.promptCounter.textContent = `Prompt ${state.promptIndex + 1} of ${state.prompts.length}`;
    if (state.randomOrder && state.randomOrder.length) {
        state.randomOrder = state.randomOrder.filter(idx => idx !== state.promptIndex);
    }
    if (elements.jumpPromptInput) {
        elements.jumpPromptInput.focus();
        elements.jumpPromptInput.select();
    }
}

function floatToPcm16(samples) {
    const output = new ArrayBuffer(samples.length * 2);
    const view = new DataView(output);
    for (let index = 0; index < samples.length; index += 1) {
        const value = Math.max(-1, Math.min(1, samples[index]));
        view.setInt16(index * 2, value < 0 ? value * 0x8000 : value * 0x7fff, true);
    }
    return output;
}

async function sha256Hex(buffer) {
    const hash = await crypto.subtle.digest('SHA-256', buffer);
    return Array.from(new Uint8Array(hash), byte => byte.toString(16).padStart(2, '0')).join('');
}

function enqueueChunk(samples) {
    if (!state.sessionId || state.uploadError) return;
    const pcm = floatToPcm16(samples);
    if (state.queuedBytes + pcm.byteLength > state.maxQueuedBytes) {
        state.uploadError = new Error('Upload is slower than recording. Recording stopped to protect memory.');
        showError(state.uploadError.message);
        stopRecording();
        return;
    }
    state.queue.push({ sequence: state.nextSequence++, pcm });
    state.queuedBytes += pcm.byteLength;
    pumpUploadQueue();
}

async function pumpUploadQueue() {
    if (state.uploading || state.uploadError) return;
    state.uploading = true;
    try {
        while (state.queue.length) {
            const item = state.queue[0];
            const checksum = await sha256Hex(item.pcm);
            await api(`/api/v1/sessions/${state.sessionId}/chunks/${item.sequence}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/octet-stream',
                    'X-Chunk-SHA256': checksum,
                },
                body: item.pcm,
            });
            state.queue.shift();
            state.queuedBytes -= item.pcm.byteLength;
        }
    } catch (error) {
        state.uploadError = error;
        showError(`Upload interrupted: ${error.message}`);
        if (state.phase === 'recording') stopRecording();
    } finally {
        state.uploading = false;
        const waiters = state.uploadWaiters.splice(0);
        waiters.forEach(resolve => resolve());
    }
}

async function waitForUploads() {
    while (state.uploading || state.queue.length) {
        await new Promise(resolve => state.uploadWaiters.push(resolve));
        if (!state.uploading && state.queue.length && !state.uploadError) pumpUploadQueue();
    }
    if (state.uploadError) throw state.uploadError;
}

async function startRecording() {
    clearError();
    if (!elements.promptText.value.trim()) {
        showError('Enter or select a prompt before recording.');
        return;
    }
    if (!navigator.mediaDevices?.getUserMedia || !window.AudioWorkletNode) {
        showError('This browser does not support secure microphone recording.');
        return;
    }

    setPhase('requesting', 'Requesting microphone access…');
    revokePlayback();
    try {
        const requestedRate = Number(elements.sampleRate.value);
        state.mediaStream = await navigator.mediaDevices.getUserMedia({
            audio: { channelCount: 1, echoCancellation: false, noiseSuppression: false, autoGainControl: false },
        });
        state.audioContext = new AudioContext({ sampleRate: requestedRate, latencyHint: 'interactive' });
        await state.audioContext.audioWorklet.addModule('/static/pcm-recorder-worklet.js');
        await state.audioContext.resume();

        const sessionPayload = {
            prompt: elements.promptText.value.trim(),
            sample_rate: state.audioContext.sampleRate,
        };
        if (state.pendingOverwriteRecordingId) {
            sessionPayload.overwrite_recording_id = state.pendingOverwriteRecordingId;
            state.overwriteRecordingId = state.pendingOverwriteRecordingId;
            state.pendingOverwriteRecordingId = null;
        } else {
            state.overwriteRecordingId = null;
        }

        const session = await api('/api/v1/sessions', {
            method: 'POST',
            body: JSON.stringify(sessionPayload),
        });
        state.sessionId = session.session_id;
        state.nextSequence = session.next_sequence;
        state.queue = [];
        state.queuedBytes = 0;
        state.uploadError = null;
        state.stopping = false;
        elements.elapsed.textContent = '00:00';

        state.sourceNode = state.audioContext.createMediaStreamSource(state.mediaStream);
        state.workletNode = new AudioWorkletNode(state.audioContext, 'pcm-recorder');
        state.analyser = state.audioContext.createAnalyser();
        state.analyser.fftSize = 256;
        const silentGain = state.audioContext.createGain();
        silentGain.gain.value = 0;
        state.sourceNode.connect(state.analyser);
        state.sourceNode.connect(state.workletNode).connect(silentGain).connect(state.audioContext.destination);
        state.workletNode.port.onmessage = handleWorkletMessage;
        state.mediaStream.getTracks().forEach(track => {
            track.addEventListener('ended', () => {
                if (state.phase === 'recording') {
                    showError('The microphone became unavailable.');
                    stopRecording();
                }
            });
        });

        state.startedAt = performance.now();
        state.timer = window.setInterval(updateTimer, 250);
        drawMeter();
        setPhase('recording', `Recording at ${state.audioContext.sampleRate.toLocaleString()} Hz.`);
    } catch (error) {
        await cleanupMedia();
        if (state.sessionId) await abortSessionQuietly();
        setPhase('ready', 'Ready to try again.');
        showError(error.name === 'NotAllowedError' ? 'Microphone permission was denied.' : error.message);
    }
}

function handleWorkletMessage(event) {
    if (event.data?.type === 'chunk') enqueueChunk(event.data.samples);
}

function updateTimer() {
    const elapsedSeconds = (performance.now() - state.startedAt) / 1000;
    elements.elapsed.textContent = formatTime(elapsedSeconds);
    if (elapsedSeconds >= state.maxDurationSeconds) {
        elements.status.textContent = 'Maximum duration reached. Stopping…';
        stopRecording();
    }
}

function drawMeter() {
    if (!state.analyser || state.phase !== 'recording') return;
    const values = new Uint8Array(state.analyser.frequencyBinCount);
    state.analyser.getByteTimeDomainData(values);
    let energy = 0;
    for (const value of values) {
        const normalized = (value - 128) / 128;
        energy += normalized * normalized;
    }
    const rms = Math.sqrt(energy / values.length);
    elements.meter.style.width = `${Math.min(100, Math.max(2, rms * 320))}%`;
    state.animationFrame = requestAnimationFrame(drawMeter);
}

async function flushWorklet() {
    if (!state.workletNode) return;
    await new Promise(resolve => {
        const timeout = setTimeout(resolve, 500);
        const listener = event => {
            if (event.data?.type === 'flushed') {
                clearTimeout(timeout);
                state.workletNode.port.removeEventListener('message', listener);
                resolve();
            }
        };
        state.workletNode.port.addEventListener('message', listener);
        state.workletNode.port.start();
        state.workletNode.port.postMessage({ type: 'flush' });
    });
}

async function stopRecording() {
    if (state.stopping || state.phase !== 'recording') return;
    state.stopping = true;
    setPhase('stopping', 'Stopping microphone and flushing audio…');
    try {
        await flushWorklet();
        await cleanupMedia();
        await waitForUploads();
        setPhase('review', 'Capture complete. Save it or discard and try again.');
    } catch (error) {
        setPhase('error', 'The recording could not be completed.');
        showError(error.message);
    } finally {
        state.stopping = false;
    }
}

async function cleanupMedia() {
    if (state.timer) clearInterval(state.timer);
    if (state.animationFrame) cancelAnimationFrame(state.animationFrame);
    state.timer = null;
    state.animationFrame = null;
    elements.meter.style.width = '2%';
    state.mediaStream?.getTracks().forEach(track => track.stop());
    state.sourceNode?.disconnect();
    state.workletNode?.disconnect();
    if (state.audioContext && state.audioContext.state !== 'closed') await state.audioContext.close();
    state.mediaStream = null;
    state.sourceNode = null;
    state.workletNode = null;
    state.analyser = null;
    state.audioContext = null;
}

async function saveRecording() {
    if (!state.sessionId || state.phase !== 'review') return;
    clearError();
    setPhase('uploading', 'Finalizing WAV file…');
    try {
        const completePayload = {};
        if (state.overwriteRecordingId) {
            completePayload.overwrite_recording_id = state.overwriteRecordingId;
        }
        const recording = await api(`/api/v1/sessions/${state.sessionId}/complete`, {
            method: 'POST',
            body: JSON.stringify(completePayload),
        });
        state.recordingId = recording.recording_id;
        const audioResponse = await fetch(recording.audio_url, {
            cache: 'no-store',
        });
        if (!audioResponse.ok) throw new Error('Saved audio could not be loaded for review.');
        const audioBlob = await audioResponse.blob();
        revokePlayback();
        state.playbackUrl = URL.createObjectURL(audioBlob);
        elements.playback.src = state.playbackUrl;
        elements.savedDuration.textContent = `${recording.duration_seconds.toFixed(2)} seconds`;
        elements.savedRate.textContent = `${recording.sample_rate.toLocaleString()} Hz`;
        elements.reviewPanel.hidden = false;
        state.sessionId = null;
        const currentPrompt = elements.promptText.value;
        const currentPromptIndex = state.promptIndex;
        const currentPromptCounter = elements.promptCounter.textContent;
        state.lastSaved = {
            recordingId: recording.recording_id,
            prompt: recording.prompt || currentPrompt,
            promptIndex: currentPromptIndex,
            promptCounterText: currentPromptCounter,
        };
        state.overwriteRecordingId = null;
        setPhase('saved', 'Recording saved successfully.');
        nextPrompt(false);
    } catch (error) {
        setPhase('review', 'Finalization failed. Retry or discard this capture.');
        showError(error.message);
    }
}

function retryRecording() {
    if (!state.lastSaved) return;
    const toRetry = state.lastSaved;
    if (elements.playback) {
        elements.playback.pause();
    }
    revokePlayback();
    elements.promptText.value = toRetry.prompt;
    if (toRetry.promptIndex >= 0) {
        state.promptIndex = toRetry.promptIndex;
        if (state.prompts.length) {
            elements.promptCounter.textContent = toRetry.promptCounterText || `Prompt ${state.promptIndex + 1} of ${state.prompts.length}`;
        }
    }
    state.pendingOverwriteRecordingId = toRetry.recordingId;
    elements.elapsed.textContent = '00:00';
    clearError();
    setPhase('ready', 'Retrying previous recording. Ready to record (saving will overwrite previous file).');
    elements.recordButton.scrollIntoView({ behavior: 'smooth', block: 'center' });
    elements.recordButton.focus();
}

async function abortSessionQuietly() {
    if (!state.sessionId) return;
    try {
        await api(`/api/v1/sessions/${state.sessionId}`, { method: 'DELETE' });
    } catch (_) {}
    state.sessionId = null;
}

async function discardRecording() {
    if (state.phase === 'recording') await stopRecording();
    await cleanupMedia();
    await abortSessionQuietly();
    state.queue = [];
    state.queuedBytes = 0;
    state.uploadError = null;
    state.nextSequence = 0;
    state.pendingOverwriteRecordingId = null;
    state.overwriteRecordingId = null;
    elements.elapsed.textContent = '00:00';
    clearError();
    setPhase('ready', 'Draft discarded. Ready to record.');
}

function revokePlayback() {
    if (state.playbackUrl) URL.revokeObjectURL(state.playbackUrl);
    state.playbackUrl = null;
    elements.playback.removeAttribute('src');
    elements.playback.load();
    elements.reviewPanel.hidden = true;
}

async function initialize() {
    try {
        const [config, promptData] = await Promise.all([
            api('/api/v1/config'),
            api('/api/v1/prompts'),
        ]);
        state.maxDurationSeconds = config.max_duration_seconds;
        state.prompts = promptData.prompts || [];
        elements.limit.textContent = `/ ${formatTime(state.maxDurationSeconds)}`;
        elements.serviceBadge.textContent = 'Service ready';
        elements.serviceBadge.classList.add('ready');
        if (elements.backToMainButton && document.referrer) {
            try {
                const refUrl = new URL(document.referrer);
                if (refUrl.pathname.includes('dashboard') || refUrl.port === '5000') {
                    elements.backToMainButton.href = document.referrer;
                }
            } catch {
                // Keep default href
            }
        }
        if (elements.jumpPromptInput && state.prompts.length) {
            elements.jumpPromptInput.max = state.prompts.length;
        }
        if (state.prompts.length) nextPrompt();
        setPhase('ready', 'Ready to request microphone access.');
    } catch (error) {
        setPhase('unauthorized', 'Recorder access failed.');
        showError(error.message);
    }
}

elements.recordButton.addEventListener('click', () => {
    if (state.phase === 'recording') stopRecording();
    else startRecording();
});
elements.saveButton.addEventListener('click', saveRecording);
elements.retryButton?.addEventListener('click', retryRecording);
elements.discardButton.addEventListener('click', discardRecording);
elements.nextPromptButton.addEventListener('click', nextPrompt);
elements.jumpPromptButton?.addEventListener('click', jumpToPrompt);
elements.jumpPromptInput?.addEventListener('keydown', event => {
    if (event.key === 'Enter') {
        event.preventDefault();
        jumpToPrompt();
    }
});
elements.promptMode.addEventListener('change', () => {
    state.promptIndex = -1;
    state.randomOrder = [];
    nextPrompt();
});
window.addEventListener('beforeunload', event => {
    if (state.sessionId && !['saved', 'ready'].includes(state.phase)) {
        event.preventDefault();
        event.returnValue = '';
    }
});
window.addEventListener('pagehide', () => {
    state.mediaStream?.getTracks().forEach(track => track.stop());
    revokePlayback();
});

initialize();
