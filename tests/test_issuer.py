from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from solace_obo_poc.issuer import create_app, load_keypair, main
from solace_obo_poc.obo_token import JwksVerifier, TokenError


@pytest.fixture
def client(keypair, token_cfg):
    return TestClient(create_app(keypair, token_cfg))


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_jwks_is_served(client, keypair):
    assert client.get("/.well-known/jwks.json").json() == keypair.jwks()


def test_token_endpoint_returns_verifiable_token(client, token_cfg):
    resp = client.post("/token", json={"sender_id": "alice", "actor": "poc"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] == token_cfg.ttl_seconds
    jwks = client.get("/.well-known/jwks.json").json()
    claims = JwksVerifier(jwks, issuer=token_cfg.issuer, audience=token_cfg.audience).verify(
        body["access_token"]
    )
    assert claims.subject == "alice"
    assert claims.actor == "poc"


@pytest.mark.parametrize("sender_id", ["", "a/b", "*", ">", "x" * 65])
def test_token_endpoint_rejects_unsafe_sender_id(client, sender_id):
    resp = client.post("/token", json={"sender_id": sender_id})
    assert resp.status_code == 400
    assert "sender_id" in resp.json()["detail"]


def test_token_endpoint_rejects_unsafe_actor(client):
    resp = client.post("/token", json={"sender_id": "alice", "actor": "a/b"})
    assert resp.status_code == 400
    assert "actor" in resp.json()["detail"]


def test_token_endpoint_requires_sender_id(client):
    assert client.post("/token", json={}).status_code == 422


def test_load_keypair_generates_when_no_file():
    assert load_keypair(None).kid


def test_load_keypair_missing_file_is_actionable(tmp_path: Path):
    with pytest.raises(TokenError, match="--key-file"):
        load_keypair(tmp_path / "missing.pem")


def test_main_returns_2_on_bad_key_file(tmp_path: Path):
    assert main(["--key-file", str(tmp_path / "missing.pem")]) == 2
