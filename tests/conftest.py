from __future__ import annotations

from collections.abc import Callable

import pytest

from solace_obo_poc.config import TokenConfig
from solace_obo_poc.obo_token import JwksVerifier, KeyPair


@pytest.fixture(scope="session")
def keypair() -> KeyPair:
    return KeyPair.generate(kid="test-kid")


@pytest.fixture(scope="session")
def token_cfg() -> TokenConfig:
    return TokenConfig(
        issuer_url="http://issuer.test",
        issuer="test-issuer",
        audience="poc",
        ttl_seconds=300,
    )


@pytest.fixture(scope="session")
def verifier(keypair: KeyPair, token_cfg: TokenConfig) -> JwksVerifier:
    return JwksVerifier(keypair.jwks(), issuer=token_cfg.issuer, audience=token_cfg.audience)


@pytest.fixture
def issue(keypair: KeyPair, token_cfg: TokenConfig) -> Callable[..., str]:
    """issue(subject, **overrides) -> token string signed by the session keypair."""

    def _issue(subject: str = "alice", **overrides) -> str:
        kwargs = dict(
            issuer=token_cfg.issuer,
            audience=token_cfg.audience,
            subject=subject,
            actor="poc-publisher",
            ttl_seconds=token_cfg.ttl_seconds,
        )
        kwargs.update(overrides)
        return keypair.issue(**kwargs)

    return _issue
