"""Tests for services/app.py: Application factory, routing, auth, and blueprint namespacing."""

import pytest
from services.app import create_app
from services.extensions import db
from services.models import User


@pytest.fixture
def app():
    test_config = {
        "TESTING": True,
        "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
        "WTF_CSRF_ENABLED": False,
        "SECRET_KEY": "test-secret-key",
    }
    app = create_app(test_config=test_config)

    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


def test_blueprint_registration(app):
    assert "annotation" in app.blueprints
    rule_paths = [rule.rule for rule in app.url_map.iter_rules()]
    assert "/annotation/" in rule_paths
    assert "/annotation/api/config" in rule_paths
    assert "/annotation/api/state" in rule_paths


def test_public_routes(client):
    res_login = client.get("/login")
    assert res_login.status_code == 200
    assert b"Platform Login" in res_login.data or b"Sign In" in res_login.data

    res_register = client.get("/register")
    assert res_register.status_code == 200
    assert b"Create Account" in res_register.data


def test_unauthenticated_redirects(client):
    assert client.get("/").status_code == 302
    assert "/login" in client.get("/").headers["Location"]

    assert client.get("/dashboard").status_code == 302
    assert "/login" in client.get("/dashboard").headers["Location"]

    assert client.get("/annotation/").status_code == 302
    assert "/login" in client.get("/annotation/").headers["Location"]


def test_legacy_redirects(client):
    res = client.get("/annotate")
    assert res.status_code == 302
    assert "/annotation/" in res.headers["Location"]

    res_exp = client.get("/export_dataset")
    assert res_exp.status_code == 302
    assert "/annotation/export_dataset" in res_exp.headers["Location"]


def test_authenticated_flow_and_api(app, client):
    # Register user
    res_reg = client.post(
        "/register",
        data={"email": "annotator1@example.com", "password": "password123"},
        follow_redirects=True,
    )
    assert res_reg.status_code == 200

    # Login user
    res_login = client.post(
        "/login",
        data={"email": "annotator1@example.com", "password": "password123"},
        follow_redirects=True,
    )
    assert res_login.status_code == 200

    # Access Dashboard
    res_dash = client.get("/dashboard")
    assert res_dash.status_code == 200
    assert b"Telegram Scraping & Annotation Overview" in res_dash.data

    # Access Annotation UI
    res_ann = client.get("/annotation/")
    assert res_ann.status_code == 200
    assert b"ANNOTATION_API_BASE" in res_ann.data

    # Access Annotation API Config
    res_cfg = client.get("/annotation/api/config")
    assert res_cfg.status_code == 200
    data = res_cfg.get_json()
    assert data["annotator"] == "annotator1"
    assert "fields" in data
    assert "available_languages" in data
