"""OBO token issue and verification.

The token is an RS256 JWT. `sub` is the end user the event is sent on behalf of
(the sender-id), `act.sub` is the service presenting it (RFC 8693 shape). The
issuer publishes its public key as a JWKS; every other hop verifies against
that and never sees the private key.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

TOKEN_PROPERTY = "obo-token"  # Solace message user property that carries the token
ALGORITHM = "RS256"
_REQUIRED_CLAIMS = ["exp", "iat", "sub", "jti", "aud", "iss"]


class TokenError(Exception):
    """The token is missing, malformed, or failed verification."""


@dataclass(frozen=True)
class OboClaims:
    subject: str  # end user (sender-id) the token was issued on behalf of
    actor: str  # service presenting the token
    token_id: str
    issued_at: datetime
    expires_at: datetime


def mask_token(token: str | None) -> str:
    """Log-safe rendering: enough to correlate, never enough to replay."""
    if not token:
        return "<none>"
    return token[:8] + "..." if len(token) > 8 else "***"


class KeyPair:
    """The issuer's signing key. Generated per process unless loaded from PEM."""

    def __init__(self, private_key: rsa.RSAPrivateKey, kid: str) -> None:
        self._private_key = private_key
        self.kid = kid

    @classmethod
    def generate(cls, kid: str | None = None) -> KeyPair:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        return cls(key, kid or uuid.uuid4().hex)

    @classmethod
    def from_pem(cls, pem: bytes, kid: str) -> KeyPair:
        key = serialization.load_pem_private_key(pem, password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise TokenError("issuer key must be an RSA private key in PEM form")
        return cls(key, kid)

    def jwks(self) -> dict[str, Any]:
        jwk = RSAAlgorithm.to_jwk(self._private_key.public_key(), as_dict=True)
        jwk.update({"kid": self.kid, "use": "sig", "alg": ALGORITHM})
        return {"keys": [jwk]}

    def issue(
        self,
        *,
        issuer: str,
        audience: str,
        subject: str,
        actor: str,
        ttl_seconds: int,
        now: datetime | None = None,
    ) -> str:
        now = now or datetime.now(UTC)
        iat = int(now.timestamp())
        claims = {
            "iss": issuer,
            "aud": audience,
            "sub": subject,
            "act": {"sub": actor},
            "obo": True,
            "iat": iat,
            "exp": iat + ttl_seconds,
            "jti": uuid.uuid4().hex,
        }
        return jwt.encode(claims, self._private_key, algorithm=ALGORITHM, headers={"kid": self.kid})


class JwksVerifier:
    """Verifies tokens against a JWKS document; one instance per issuer."""

    def __init__(self, jwks: dict[str, Any], *, issuer: str, audience: str) -> None:
        try:
            self._keys = jwt.PyJWKSet.from_dict(jwks)
        except jwt.PyJWTError as exc:
            raise TokenError(f"issuer JWKS is unusable: {exc}") from exc
        self._issuer = issuer
        self._audience = audience

    @classmethod
    def from_url(
        cls, url: str, *, issuer: str, audience: str, client: httpx.Client | None = None
    ) -> JwksVerifier:
        try:
            response = (client or httpx).get(url, timeout=10.0)
            response.raise_for_status()
            jwks = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TokenError(
                f"cannot fetch JWKS from {url}: {exc}; is the issuer running "
                "and OBO_ISSUER_URL pointing at it?"
            ) from exc
        return cls(jwks, issuer=issuer, audience=audience)

    def verify(self, token: str | None) -> OboClaims:
        if not token:
            raise TokenError(f"no OBO token: message property {TOKEN_PROPERTY!r} is missing")
        try:
            kid = jwt.get_unverified_header(token).get("kid")
            if not kid:
                raise TokenError("token header has no kid")
            try:
                key = self._keys[kid].key
            except KeyError:
                raise TokenError(f"token signed with unknown kid {kid!r}") from None
            payload = jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],
                audience=self._audience,
                issuer=self._issuer,
                options={"require": _REQUIRED_CLAIMS},
            )
        except jwt.PyJWTError as exc:
            raise TokenError(f"OBO token {mask_token(token)} rejected: {exc}") from exc
        actor = (payload.get("act") or {}).get("sub", "")
        return OboClaims(
            subject=payload["sub"],
            actor=actor,
            token_id=payload["jti"],
            issued_at=datetime.fromtimestamp(payload["iat"], tz=UTC),
            expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        )
