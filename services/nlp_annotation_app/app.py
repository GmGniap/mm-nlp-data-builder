"""
services/nlp_annotation_app/app.py
===================================
DEPRECATED: The main Flask entrypoint has been relocated to services/app.py.

This module is retained for backward compatibility with existing run scripts and imports.
Please update entrypoint references to use `services.app`.
"""

from __future__ import annotations

import warnings
import sys
from pathlib import Path

_SERVICES_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = _SERVICES_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

warnings.warn(
    "Running services/nlp_annotation_app/app.py is deprecated. "
    "Please run 'python services/app.py' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from services.app import app, create_app  # noqa: F401

if __name__ == '__main__':
    print(
        "\n[NOTICE] The main application has moved to services/app.py.\n"
        "Starting application via services.app...\n"
    )
    
