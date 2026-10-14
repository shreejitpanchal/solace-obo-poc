from datetime import UTC, datetime, timedelta

import httpx
import pytest

from solace_obo_poc.obo_token import JwksVerifier, KeyPair, TokenError, mask_token


def test_issue_and_verify_round_trip(issue, verifier):
    token = issue("alice", actor="poc-publisher")
    claims = verifier.verify(token)
    assert claims.subject == "alice"
    assert claims.actor == "poc-publisher"
    assert claims.token_id
    assert claims.expires_at - claims.issued_at == timedelta(seconds=300)


def test_missing_token_is_rejected_before_any_parsing(verifier):
    with pytest.raises(TokenError, match="obo-token"):
        verifier.verify(None)
    with pytest.raises(TokenError, match="obo-token"):
        verifier.verify("")


def test_expired_token_is_rejected(issue, verifier):
    token = issue("alice", now=datetime.now(UTC) - timedelta(seconds=600))
    with pytest.raises(TokenError, match="expired"):
        verifier.verify(token)


def test_wrong_audience_is_rejected(issue, verifier):
    with pytest.raises(TokenError, match="udience"):
        verifier.verify(issue("alice", audience="someone-else"))


def test_wrong_issuer_is_rejected(issue, verifier):
    with pytest.raises(TokenError, match="ssuer"):
        verifier.verify(issue("alice", issuer="rogue"))


def test_tampered_signature_is_rejected(issue, verifier):
    token = issue("alice")
    head, body, sig = token.split(".")
    tampered = ".".join([head, body, sig[:-4] + ("AAAA" if sig[-4:] != "AAAA" else "BBBB")])
    with pytest.raises(TokenError, match="rejected"):
        verifier.verify(tampered)


def test_token_from_unknown_key_is_rejected(verifier, token_cfg):
    other = KeyPair.generate(kid="other-kid")
    token = other.issue(
        issuer=token_cfg.issuer,
        audience=token_cfg.audience,
        subject="alice",
        actor="x",
        ttl_seconds=60,
    )
    with pytest.raises(TokenError, match="unknown kid"):
        verifier.verify(token)


def test_garbage_token_is_rejected(verifier):
    with pytest.raises(TokenError):
        verifier.verify("not.a.jwt")


def test_jwks_document_shape(keypair):
    jwks = keypair.jwks()
    assert len(jwks["keys"]) == 1
    key = jwks["keys"][0]
    assert key["kid"] == "test-kid"
    assert key["kty"] == "RSA"
    assert key["alg"] == "RS256"
    assert "d" not in key  # private exponent never leaves the issuer


def test_unusable_jwks_fails_loudly():
    with pytest.raises(TokenError, match="JWKS"):
        JwksVerifier({"keys": []}, issuer="i", audience="a")


def test_from_url_fetches_jwks(keypair, token_cfg, issue):
    def app(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/.well-known/jwks.json"
        return httpx.Response(200, json=keypair.jwks())

    client = httpx.Client(transport=httpx.MockTransport(app))
    verifier = JwksVerifier.from_url(
        token_cfg.jwks_url, issuer=token_cfg.issuer, audience=token_cfg.audience, client=client
    )
    assert verifier.verify(issue("bob")).subject == "bob"


def test_from_url_error_names_the_fix(token_cfg):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    with pytest.raises(TokenError, match="OBO_ISSUER_URL"):
        JwksVerifier.from_url(token_cfg.jwks_url, issuer="i", audience="a", client=client)


def test_from_pem_rejects_non_rsa_keys():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    pem = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    with pytest.raises(TokenError, match="RSA"):
        KeyPair.from_pem(pem, kid="k")


def test_from_pem_round_trips_rsa_key(keypair, token_cfg, verifier):
    from cryptography.hazmat.primitives import serialization

    pem = keypair._private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    reloaded = KeyPair.from_pem(pem, kid="test-kid")
    token = reloaded.issue(
        issuer=token_cfg.issuer, audience=token_cfg.audience, subject="a", actor="b", ttl_seconds=5
    )
    assert verifier.verify(token).subject == "a"


def test_mask_token_never_reveals_the_body():
    assert mask_token(None) == "<none>"
    assert mask_token("short") == "***"
    assert mask_token("abcdefghijklmnop") == "abcdefgh..."
