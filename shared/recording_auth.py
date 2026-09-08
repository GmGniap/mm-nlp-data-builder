"""Signed identity tokens shared by the annotation and recording services."""

from __future__ import annotations

from typing import Any

from itsdangerous import BadData, URLSafeTimedSerializer


TOKEN_SALT = "mm-nlp-recording-service-v1"


def create_recording_token(secret: str, user_id: int | str, email: str) -> str:
    serializer = URLSafeTimedSerializer(secret_key=secret, salt=TOKEN_SALT)
    return serializer.dumps({"user_id": str(user_id), "email": email})


def verify_recording_token(secret: str, token: str, max_age: int) -> dict[str, Any]:
    serializer = URLSafeTimedSerializer(secret_key=secret, salt=TOKEN_SALT)
    try:
        payload = serializer.loads(token, max_age=max_age)
    except BadData as exc:
        raise ValueError("The recording access token is invalid or expired.") from exc

    if not isinstance(payload, dict) or not payload.get("user_id"):
        raise ValueError("The recording access token has an invalid payload.")
    return payload
