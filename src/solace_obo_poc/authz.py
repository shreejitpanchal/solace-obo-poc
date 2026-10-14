"""Authorize-before-read gate shared by every consumer.

The payload is not decoded, and the decoder is not even called, until the OBO
token has been verified and its subject matches the sender-id in the topic.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, OboClaims, TokenError
from solace_obo_poc.topics import TopicParts, parse_topic


class AuthorizationError(Exception):
    """The event must not be read. The message says why; the payload stays untouched."""


class PayloadError(Exception):
    """Authorized, but the payload is not what the publisher contract promises."""


@dataclass(frozen=True)
class AuthorizedEvent:
    topic: TopicParts
    claims: OboClaims
    payload: Any


def authorize_event(
    topic: str,
    properties: Mapping[str, str],
    raw_payload: bytes | str,
    verifier: JwksVerifier,
    *,
    decode: Callable[[bytes | str], Any] = json.loads,
) -> AuthorizedEvent:
    try:
        parts = parse_topic(topic)
    except ValueError as exc:
        raise AuthorizationError(f"refusing event: {exc}") from exc
    try:
        claims = verifier.verify(properties.get(TOKEN_PROPERTY))
    except TokenError as exc:
        raise AuthorizationError(f"refusing event on {topic}: {exc}") from exc
    if claims.subject != parts.sender_id:
        raise AuthorizationError(
            f"refusing event on {topic}: token subject {claims.subject!r} does not match "
            f"sender-id {parts.sender_id!r} in the topic"
        )
    # Only now is the payload touched.
    try:
        payload = decode(raw_payload)
    except ValueError as exc:
        raise PayloadError(f"event on {topic} authorized but payload is not JSON: {exc}") from exc
    return AuthorizedEvent(topic=parts, claims=claims, payload=payload)
