"""POC validator: verify the OBO token on raw events, republish them as validated.

Subscribes to poc/msg/>, checks the token against the issuer's JWKS and
that its subject is the sender-id in the topic, then republishes the payload
unchanged to poc/msg-validate/{sender-id}/{thread-id} with the token
carried along, so downstream consumers can re-authorize independently.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from solace_obo_poc.broker import Broker, Handler, Inbound, Outbound
from solace_obo_poc.config import broker_config, configure_logging, token_config
from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, TokenError, mask_token
from solace_obo_poc.topics import parse_topic, raw_subscription, validated_topic, validator_queue

log = logging.getLogger(__name__)

VALIDATED_AT_PROPERTY = "obo-validated-at"


def validate_message(msg: Inbound, verifier: JwksVerifier) -> Outbound:
    """Return the validated republish, or raise TokenError/ValueError saying why not."""
    parts = parse_topic(msg.topic)
    if not parts.is_raw:
        raise ValueError(f"validator only accepts raw topics, got {msg.topic!r}")
    claims = verifier.verify(msg.properties.get(TOKEN_PROPERTY))
    if claims.subject != parts.sender_id:
        raise TokenError(
            f"token subject {claims.subject!r} does not match sender-id {parts.sender_id!r}"
        )
    properties = dict(msg.properties)
    properties[VALIDATED_AT_PROPERTY] = datetime.now(UTC).isoformat()
    return Outbound(
        topic=validated_topic(parts.sender_id, parts.thread_id),
        payload=msg.payload,
        properties=properties,
    )


def make_handler(publish: Callable[[Outbound], None], verifier: JwksVerifier) -> Handler:
    """One bad message is logged and dropped; the receiver keeps running."""

    def handle(msg: Inbound) -> None:
        try:
            out = validate_message(msg, verifier)
        except (TokenError, ValueError) as exc:
            log.warning(
                "dropped %s (token %s): %s",
                msg.topic,
                mask_token(msg.properties.get(TOKEN_PROPERTY)),
                exc,
            )
            return
        publish(out)
        log.info("validated %s -> %s", msg.topic, out.topic)

    return handle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="POC validator service")
    parser.parse_args(argv)
    configure_logging()
    cfg = token_config()
    try:
        verifier = JwksVerifier.from_url(cfg.jwks_url, issuer=cfg.issuer, audience=cfg.audience)
    except TokenError as exc:
        log.error("%s", exc)
        return 2
    broker_cfg = broker_config()
    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(
            validator_queue(broker_cfg.queue_prefix),
            [raw_subscription()],
            make_handler(broker.publish, verifier),
        )
        broker.wait()
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
