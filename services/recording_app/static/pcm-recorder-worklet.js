class PcmRecorderProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.capacity = 16384;
        this.buffer = new Float32Array(this.capacity);
        this.offset = 0;
        this.port.onmessage = (event) => {
            if (event.data?.type === 'flush') {
                this.flush();
                this.port.postMessage({ type: 'flushed' });
            }
        };
    }

    flush() {
        if (this.offset === 0) return;
        const chunk = this.buffer.slice(0, this.offset);
        this.port.postMessage({ type: 'chunk', samples: chunk }, [chunk.buffer]);
        this.offset = 0;
    }

    process(inputs) {
        const channel = inputs[0]?.[0];
        if (!channel) return true;

        let cursor = 0;
        while (cursor < channel.length) {
            const count = Math.min(channel.length - cursor, this.capacity - this.offset);
            this.buffer.set(channel.subarray(cursor, cursor + count), this.offset);
            this.offset += count;
            cursor += count;
            if (this.offset === this.capacity) this.flush();
        }
        return true;
    }
}

registerProcessor('pcm-recorder', PcmRecorderProcessor);
