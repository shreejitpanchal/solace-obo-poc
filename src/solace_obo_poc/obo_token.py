"""OBO token: how it is issued and how every hop verifies it.

The token is a JWT signed with RS256 (RSA private key signs, RSA public key
verifies). That asymmetry is what makes the flow work: only the issuer holds
the private key; the validator and the applications hold public keys only,
which the issuer publishes as a JWKS document at /.well-known/jwks.json.

Claims (see also the README, "Step 1"):

    iss      who issued it            must equal OBO_ISSUER
    aud      which system it is for   must equal OBO_AUDIENCE
    sub      the END USER the event is sent on behalf of (the sender-id)
    act.sub  the SERVICE presenting the token (RFC 8693 "actor" shape)
    iat/exp  issued-at / expiry       verified; default lifetime 300 s
    jti      unique token id          for audit correlation
    kid      (header) which issuer key signed it; selects the JWKS entry

Two classes:
    KeyPair        issuer side: holds the private key, mints tokens, exports JWKS
    JwksVerifier   everyone else: holds a JWKS, verifies tokens offline

Tokens are credentials. mask_token() is the only form that may be logged.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

log = logging.getLogger(__name__)

# Name of the Solace message user property that carries the token.
TOKEN_PROPERTY = "obo-token"
ALGORITHM = "RS256"
# A token missing any of these is rejected even if its signature is valid.
_REQUIRED_CLAIMS = ["exp", "iat", "sub", "jti", "aud", "iss"]


class TokenError(Exception):
    """The token is missing, malformed, or failed verification. The message says why."""


@dataclass(frozen=True)
class OboClaims:
    """The verified content of a token, in the vocabulary of the flow."""

    subject: str  # end user (sender-id) the token was issued on behalf of
    actor: str  # service presenting the token
    token_id: str
    issued_at: datetime
    expires_at: datetime


def mask_token(token: str | None) -> str:
    """Log-safe rendering: enough to correlate with the issuer's logs, never enough to replay."""
    if not token:
        return "<none>"
    return token[:8] + "..." if len(token) > 8 else "***"


class KeyPair:
    """The issuer's signing key. Generated per process unless loaded from PEM.

    Only the issuer service instantiates this. Nothing else in the flow ever
    touches a private key.
    """

    def __init__(self, private_key: rsa.RSAPrivateKey, kid: str) -> None:
        self._private_key = private_key
        self.kid = kid  # key id written into every token header

    @classmethod
    def generate(cls, kid: str | None = None) -> KeyPair:
        """Fresh 2048-bit RSA key. A restarted issuer therefore has a NEW key,
        and tokens signed by the old one fail with "unknown kid" downstream
        until the verifiers refetch the JWKS (in the POC: restart them)."""
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        kid = kid or uuid.uuid4().hex
        log.info(
            "generated new RSA-2048 signing key kid=%s (tokens from older keys will not verify)",
            kid,
        )
        return cls(key, kid)

    @classmethod
    def from_pem(cls, pem: bytes, kid: str) -> KeyPair:
        """Load a persistent key (issuer --key-file) so tokens survive restarts."""
        key = serialization.load_pem_private_key(pem, password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise TokenError("issuer key must be an RSA private key in PEM form")
        return cls(key, kid)

    def jwks(self) -> dict[str, Any]:
        """The PUBLIC half, in the standard JSON Web Key Set shape.

        Served at /.well-known/jwks.json. Contains n and e (public modulus and
        exponent) plus kid/use/alg; never the private exponent d.
        """
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
        """Mint a token: "`actor` acts on behalf of `subject` for `audience` until now+ttl"."""
        now = now or datetime.now(UTC)
        iat = int(now.timestamp())
        claims = {
            "iss": issuer,
            "aud": audience,
            "sub": subject,
            "act": {"sub": actor},
            "obo": True,  # marker that this is an on-behalf-of token
            "iat": iat,
            "exp": iat + ttl_seconds,
            "jti": uuid.uuid4().hex,
        }
        # The kid goes into the JWT header so verifiers can pick the matching
        # JWKS entry before checking the signature.
        token = jwt.encode(
            claims, self._private_key, algorithm=ALGORITHM, headers={"kid": self.kid}
        )
        log.info(
            "issued token %s: sub=%s act=%s aud=%s jti=%s ttl=%ds kid=%s",
            mask_token(token),
            subject,
            actor,
            audience,
            claims["jti"],
            ttl_seconds,
            self.kid,
        )
        return token


class JwksVerifier:
    """Verifies tokens against a JWKS document; one instance per issuer.

    Used by the recipient and the user-profiler (the validator only forwards
    and never verifies). Verification is entirely local: after the JWKS has
    been fetched once, no network call is made per message.
    """

    def __init__(self, jwks: dict[str, Any], *, issuer: str, audience: str) -> None:
        try:
            self._keys = jwt.PyJWKSet.from_dict(jwks)
        except jwt.PyJWTError as exc:
            raise TokenError(f"issuer JWKS is unusable: {exc}") from exc
        self._issuer = issuer  # expected `iss`
        self._audience = audience  # expected `aud`
        kids = [k.get("kid") for k in jwks.get("keys", [])]
        log.info(
            "verifier ready: %d public key(s) kids=%s; will require iss=%r aud=%r",
            len(kids),
            kids,
            issuer,
            audience,
        )

    @classmethod
    def from_url(
        cls, url: str, *, issuer: str, audience: str, client: httpx.Client | None = None
    ) -> JwksVerifier:
        """Fetch the JWKS from the issuer (GET /.well-known/jwks.json) and build a verifier."""
        log.info("fetching issuer public keys from %s", url)
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
        """Full verification. Returns the claims, or raises TokenError with the reason.

        Checks, in order:
          1. a token is present at all (the message property may be missing)
          2. the header names a kid we have a public key for
          3. signature (RS256) is valid for that key
          4. aud and iss match what this deployment expects
          5. exp has not passed (and iat/sub/jti are present)
        Note what is NOT checked here: that `sub` matches the topic. That is
        the caller's job (authz.authorize_event),
        because only the caller knows the topic.
        """
        if not token:
            raise TokenError(f"no OBO token: message property {TOKEN_PROPERTY!r} is missing")
        masked = mask_token(token)
        try:
            # Read the header without trusting it yet: we only need the kid
            # to select the key. Everything else waits for jwt.decode().
            kid = jwt.get_unverified_header(token).get("kid")
            if not kid:
                raise TokenError("token header has no kid")
            log.debug("token %s: header kid=%s, selecting public key", masked, kid)
            try:
                key = self._keys[kid].key
            except KeyError:
                raise TokenError(f"token signed with unknown kid {kid!r}") from None
            log.debug(
                "token %s: checking signature (%s), iss=%r, aud=%r, expiry, required claims %s",
                masked,
                ALGORITHM,
                self._issuer,
                self._audience,
                _REQUIRED_CLAIMS,
            )
            # jwt.decode() verifies signature, audience, issuer, expiry and
            # the required claims in one call, raising a specific PyJWTError
            # for each failure (ExpiredSignatureError, InvalidAudienceError, ...).
            payload = jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],
                audience=self._audience,
                issuer=self._issuer,
                options={"require": _REQUIRED_CLAIMS},
            )
        except jwt.PyJWTError as exc:
            raise TokenError(f"OBO token {masked} rejected: {exc}") from exc
        actor = (payload.get("act") or {}).get("sub", "")
        claims = OboClaims(
            subject=payload["sub"],
            actor=actor,
            token_id=payload["jti"],
            issued_at=datetime.fromtimestamp(payload["iat"], tz=UTC),
            expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        )
        log.info(
            "token %s verified: sub=%s act=%s jti=%s expires=%s",
            masked,
            claims.subject,
            claims.actor,
            claims.token_id,
            claims.expires_at.isoformat(timespec="seconds"),
        )
        return claims
