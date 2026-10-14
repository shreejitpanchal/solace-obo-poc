"""Authorize-before-read gate used by every consuming application.

This is the single place where a consumer decides whether it may read an
event. The recipient and the user-profiler both call `authorize_event` on
every message they receive, and only get a decoded payload back if the OBO
token passed.

The order of checks is the point of the module:

    1. topic      -> which sender-id / thread-id does the event claim?
    2. token      -> is there an `obo-token` property, and does it verify?
    3. binding    -> does the token's `sub` equal the sender-id in the topic?
    4. payload    -> only now decode the business data

Steps 1 to 3 raise AuthorizationError and the raw payload is never touched:
the decoder function is not even called. Tests assert exactly that.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, OboClaims, TokenError
from solace_obo_poc.topics import TopicParts, parse_topic

log = logging.getLogger(__name__)


class AuthorizationError(Exception):
    """The event must not be read. The message says why; the payload stays untouched."""


class PayloadError(Exception):
    """The token passed, but the payload is not the JSON the publisher contract promises."""


@dataclass(frozen=True)
class AuthorizedEvent:
    """What a consumer receives once an event has passed the gate."""

    topic: TopicParts  # parsed sender-id / thread-id from the topic
    claims: OboClaims  # who the token is for (`sub`) and who presented it (`act.sub`)
    payload: Any  # the decoded business data


def authorize_event(
    topic: str,
    properties: Mapping[str, str],
    raw_payload: bytes | str,
    verifier: JwksVerifier,
    *,
    decode: Callable[[bytes | str], Any] = json.loads,
) -> AuthorizedEvent:
    """Verify the token carried in `properties`, then (and only then) decode `raw_payload`.

    `decode` is injectable so a test can prove it is never called for a
    rejected event. Production code uses json.loads.
    """
    # 1. Topic. parse_topic validates both levels and rejects anything outside
    #    poc/msg/ and poc/msg-validate/, so a stray message from an unrelated
    #    topic is refused before we look at any token.
    try:
        parts = parse_topic(topic)
    except ValueError as exc:
        raise AuthorizationError(f"refusing event: {exc}") from exc
    log.debug(
        "authz step 1/4 topic ok: sender-id=%s thread-id=%s (%s space)",
        parts.sender_id,
        parts.thread_id,
        "validated" if parts.is_validated else "raw",
    )

    # 2. Token. The verifier holds the issuer's public keys; verify() checks
    #    signature, issuer, audience, expiry and required claims, and raises
    #    TokenError (with the reason) if the property is missing or bad.
    log.debug("authz step 2/4 verifying %r property", TOKEN_PROPERTY)
    try:
        claims = verifier.verify(properties.get(TOKEN_PROPERTY))
    except TokenError as exc:
        raise AuthorizationError(f"refusing event on {topic}: {exc}") from exc

    # 3. Binding. A token is only good for the user it was issued for. Without
    #    this check, alice's valid token could be attached to an event
    #    published under bob's sender-id.
    if claims.subject != parts.sender_id:
        raise AuthorizationError(
            f"refusing event on {topic}: token subject {claims.subject!r} does not match "
            f"sender-id {parts.sender_id!r} in the topic"
        )
    log.debug("authz step 3/4 binding ok: token sub=%s == topic sender-id", claims.subject)

    # 4. Payload. Only now is the business data touched.
    try:
        payload = decode(raw_payload)
    except ValueError as exc:
        raise PayloadError(f"event on {topic} authorized but payload is not JSON: {exc}") from exc
    log.debug("authz step 4/4 payload decoded (%d bytes)", len(raw_payload))
    return AuthorizedEvent(topic=parts, claims=claims, payload=payload)
