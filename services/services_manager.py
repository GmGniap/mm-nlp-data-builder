"""Auxiliary microservice process manager.

Responsible for local development process spawning and lifecycle management
(e.g., speech recorder microservice).
"""

from __future__ import annotations

import atexit
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

SERVICES_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SERVICES_DIR.parent

_recorder_process: subprocess.Popen | None = None


def get_python_executable() -> str:
    """Find the virtualenv python binary with project dependencies."""
    candidates = [
        os.path.join(sys.prefix, 'bin', 'python'),
        os.path.join(sys.prefix, 'bin', 'python3'),
        os.environ.get('VIRTUAL_ENV', '') and os.path.join(os.environ['VIRTUAL_ENV'], 'bin', 'python'),
        str(PROJECT_ROOT / '.venv' / 'bin' / 'python'),
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return sys.executable


def is_port_in_use(port: int = 5001, host: str = '127.0.0.1') -> bool:
    """Check whether a given TCP port is currently listening."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def start_recorder_service(wait_until_ready: bool = False) -> None:
    """Ensure the recording microservice is running on port 5001."""
    global _recorder_process
    if is_port_in_use(5001):
        return

    recorder_script = SERVICES_DIR / 'recording_app' / 'app.py'
    if not recorder_script.exists():
        return

    py_exec = get_python_executable()
    print(f"Starting Recorder microservice on http://127.0.0.1:5001 using {py_exec} ...")
    _recorder_process = subprocess.Popen(
        [py_exec, str(recorder_script)],
        cwd=str(PROJECT_ROOT),
        env=os.environ.copy(),
    )

    def _cleanup():
        global _recorder_process
        if _recorder_process and _recorder_process.poll() is None:
            _recorder_process.terminate()
            try:
                _recorder_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                _recorder_process.kill()

    atexit.register(_cleanup)

    if wait_until_ready:
        for _ in range(30):
            if is_port_in_use(5001):
                break
            time.sleep(0.1)
